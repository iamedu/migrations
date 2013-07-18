(ns migrations.core
  (:require [clojure.data.csv :as csv]
            [korma.db   :as kormadb]
            [korma.core :as korma])
  (:gen-class))

(kormadb/defdb inadem-db
  {:classname   "oracle.jdbc.driver.OracleDriver" ; must be in classpath
   :subprotocol "oracle:thin"
   :user        "FPYME_LECT"
   :password    "Ks0804sdF"
   :subname     "@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=SERVFIRMA)))"
   :make-pool?  true})

(declare postal-codes)

(korma/defentity postal-codes
  (korma/table :CAT_SEPOMEX_CP))

(korma/defentity states
  (korma/pk :SCO_ID)
  (korma/table :CAT_ENTIDAD_FEDERATIVA))

(korma/defentity municipalities
  (korma/pk :SCO_ID)
  (korma/table :CAT_MUNICIPIO))

(defrecord Direccion
  [solicitud-id
   id
   calle
   codigo-postal
   colonia
   descripcion
   localidad
   numero-exterior
   numero-exterior-2
   numero-interior
   vialidad-1
   vialidad-2
   vialidad-posterior
   estado
   municipio
   tipo-asentamiento-id
   tipo-vialidad-id
   tipo-vialidad-1-id
   tipo-vialidad-2-id
   tipo-vialidad-posterior-id])

(def tipo-vialidad-mapping
  {1 41
   2 35
   3 43
   4 32
   5 42
   6 24
   7 33
   8 40
   9 38
   10 26
   11 39
   12 36
   13 27
   14 28
   15 30
   16 29
   17 34
   18 21
   19 22
   20 37
   21 31
   22 23
   23 25
   :else 42})

(def tipo-asentamiento-mapping
  {1 13
   2 25
   3 5
   4 7
   5 27
   6 12
   7 1
   8 6
   9 20
   10 9
   13 19
   14 30
   15 32
   16 8
   17 17
   18 23
   19 16
   22 3
   23 26
   24 15
   25 33
   27 11
   34 4
   :else 1})

(defn translate [data]
  (str "TRANSLATE('" (.toUpperCase data) "','ÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇ','AEIOUAEIOUAOAEIOOAEIOUC')"))

(defn translate-column [data]
  (str "TRANSLATE(UPPER(" data "),'ÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇ','AEIOUAEIOUAOAEIOOAEIOUC')"))

(defn find-normalized [table column data]
  (-> 
    (korma/select* table)
    (korma/where {(korma/raw(translate-column column)) (korma/raw (translate data))})
    (korma/exec)))

(def find-states (partial find-normalized states "NOMBRE"))

(def find-postal-codes (partial find-normalized postal-codes "CODIGO_POSTAL"))

(def find-municipalities (partial find-normalized municipalities "NOMBRE"))

(defn is-valid-addr? [{:keys [codigo-postal estado municipio]}]
  (let [not-empty? (complement empty?)]
    (and
      (not-empty? codigo-postal)
      (not-empty? estado)
      (not-empty? municipio))))

(defn find-data [{:keys [estado municipio codigo-postal tipo-asentamiento-id tipo-vialidad-id tipo-vialidad-1-id tipo-vialidad-2-id tipo-vialidad-posterior-id]}]
  {:post [(not-any? nil? %)]}
  (let [states         (find-states estado)
        municipalities (find-municipalities municipio)
        codes          (find-postal-codes codigo-postal)
        state          (first states)
        municipality   (first municipalities)
        postal-code    (first codes)
        tipo-asentamiento-id (get tipo-asentamiento-mapping tipo-asentamiento-id 1)
        tipo-vialidad-id (get tipo-vialidad-mapping tipo-vialidad-id 42)
        tipo-vialidad-1-id (get tipo-vialidad-mapping tipo-vialidad-1-id 42)
        tipo-vialidad-2-id (get tipo-vialidad-mapping tipo-vialidad-2-id 42)
        tipo-vialidad-posterior-id (get tipo-vialidad-mapping tipo-vialidad-posterior-id 42)]
    (do
      (if (nil? state)        (spit "estados.txt" (str estado "\n") :append true))
      (if (nil? municipality) (spit "municipio.txt" (str municipio "\n") :append true))
      (if (nil? postal-code)  (spit "codigos" (str codigo-postal "\n") :append true))
      [:estado (get state :SCO_ID)
       :municipio (get municipality :ID)
       :codigo-postal (get postal-code :ID)
       :tipo-asentamiento-id tipo-asentamiento-id
       :tipo-vialidad-id tipo-vialidad-id
       :tipo-vialidad-1-id tipo-vialidad-1-id
       :tipo-vialidad-2-id tipo-vialidad-2-id
       :tipo-vialidad-posterior-id tipo-vialidad-posterior-id
       :full-postal-code postal-code
       :full-municipio municipio
       :full-estado estado])))

(defn map-address [{:keys [solicitud-id
                           id
                           calle
                           codigo-postal
                           colonia
                           descripcion
                           localidad
                           numero-exterior
                           numero-exterior-2
                           numero-interior
                           vialidad-1
                           vialidad-2
                           vialidad-posterior
                           estado
                           municipio
                           tipo-asentamiento-id
                           tipo-vialidad-id
                           tipo-vialidad-1-id
                           tipo-vialidad-2-id
                           tipo-vialidad-posterior-id]
                    :as addr}]
  (try
    (apply assoc addr (find-data addr))
    (catch AssertionError e
      (do 
        (spit "not-found.edn" (prn-str addr) :append true)
        nil))))

(defn create-query [{:keys [solicitud-id
                            id
                            calle
                            codigo-postal
                            colonia
                            descripcion
                            localidad
                            numero-exterior
                            numero-exterior-2
                            numero-interior
                            vialidad-1
                            vialidad-2
                            vialidad-posterior
                            estado
                            municipio
                            tipo-asentamiento-id
                            tipo-vialidad-id
                            tipo-vialidad-1-id
                            tipo-vialidad-2-id
                            tipo-vialidad-posterior-id
                            full-postal-code]
                     :or {calle              "Calle"
                          localidad          "Localidad"
                          numero-exterior    "Num. Ext."
                          numero-exterior-2  "Num. Ext. 2"
                          vialidad-1         "Vialidad 1"
                          vialidad-2         "Vialidad 2"
                          vialidad-posterior "Vialidad posterior"}
                     :as addr}]
  (identity 1))

(defn create-addresses [addresses]
  (let [filtered-addresses (remove nil? (map map-address addresses))]
    (map #(create-query %) filtered-addresses)))
;(map identity filtered-addresses)))

(defn convert-to-long [n]
  (try
    (Long/parseLong n)
    (catch Exception e nil)))

(defn convert-to-empty [n]
  (if (not (empty? n)) n nil))

(defn reify-types [addr]
  (let [long-types  [:id :solicitud-id :tipo-asentamiento-id :tipo-vialidad-id :tipo-vialidad-1-id :tipo-vialidad-2-id :tipo-vialidad-posterior-id]
        empty-types [:calle :localidad :numero-exterior :numero-exterior-2 :vialidad-1 :vialidad-2 :vialidad-posterior]
        long-converted  (reduce #(assoc %1 %2 (convert-to-long  (get %1 %2))) addr long-types)
        empty-converted (reduce #(assoc %1 %2 (convert-to-empty (get %1 %2))) long-converted empty-types)]
    empty-converted))

(defn read-addr [file]
  (filter is-valid-addr? (map #(reify-types (apply ->Direccion %)) (csv/read-csv (slurp file)))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  (alter-var-root #'*read-eval* (constantly false))
  (println "Hello, World!"))
