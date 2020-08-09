(ns metabase.driver.h2tcp
  "H2TCP driver. Builds off of the SQL-JDBC driver."
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db
             [jdbc-protocols :as jdbc-protocols]
             [spec :as dbspec]]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.plugins.classloader :as classloader]
            [metabase.query-processor
             [error-type :as error-type]
             [store :as qp.store]]
            [metabase.util
             [honeysql-extensions :as hx]
             [i18n :refer [deferred-tru tru]]]
            [metabase.util.ssh :as ssh]
            [metabase.db.spec :as db.spec]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common])
  (:import [java.sql Clob ResultSet ResultSetMetaData]
           java.time.OffsetTime))

(driver/register! :h2tcp, :parent :sql-jdbc)

(defmethod driver/display-name :h2tcp [_] "H2TCP")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(doseq [[feature supported?] {:full-join               false
                              :regex                   false
                              :percentile-aggregations false}]
  (defmethod driver/supports? [:h2tcp feature] [_ _] supported?))

;(defmethod driver/connection-properties :h2tcp
;  [_]
;  [{:name         "db"
;    :display-name (tru "Connection String")
;    :placeholder  (str "file:/" (deferred-tru "Users/camsaul/bird_sightings/toucans"))
;    :required     true}])

(defmethod driver/connection-properties :h2tcp
  [_]
  (ssh/with-tunnel-config
    [driver.common/default-host-details
     (assoc driver.common/default-port-details :placeholder 9092)
     (assoc driver.common/default-dbname-details :placeholder "xsqltest")
     (assoc driver.common/default-user-details :placeholder "sa")
     (assoc driver.common/default-password-details :placeholder "sa")
     (assoc driver.common/default-additional-options-details
       :placeholder  "OPTION=100")
     ]))

;; TODO - it would be better not to put all the options in the connection string in the first place?
(defn- connection-string->file+options
  "Explode a `connection-string` like `file:my-db;OPTION=100;OPTION_2=TRUE` to a pair of file and an options map.

    (connection-string->file+options \"file:my-crazy-db;OPTION=100;OPTION_X=TRUE\")
      -> [\"file:my-crazy-db\" {\"OPTION\" \"100\", \"OPTION_X\" \"TRUE\"}]"
  [^String connection-string]
  {:pre [(string? connection-string)]}
  (let [[file & options] (str/split connection-string #";+")
        options          (into {} (for [option options]
                                    (str/split option #"=")))]
    [file options]))

(defn- db-details->user [{:keys [db], :as details}]
  {:pre [(string? db)]}
  (pr db)
  (or (some (partial get details) ["USER" :USER])
      (let [[_ {:strs [USER]}] (connection-string->file+options db)]
        USER)))

(defn- check-native-query-not-using-default-user [{query-type :type, database-id :database, :as query}]
  (u/prog1 query
    ;; For :native queries check to make sure the DB in question has a (non-default) NAME property specified in the
    ;; connection string. We don't allow SQL execution on H2 databases for the default admin account for security
    ;; reasons
    (when (= (keyword query-type) :native)
      (let [{:keys [details]} (qp.store/database)
            user              (db-details->user details)]
        (when (or (str/blank? user)
                  (= user "sa"))        ; "sa" is the default USER
          (throw
           (ex-info (tru "Running SQL queries against H2 databases using the default (admin) database user is forbidden.")
             {:type error-type/db})))))))

(defmethod driver/execute-reducible-query :h2tcp
  [driver query chans respond]
  ;(check-native-query-not-using-default-user query)
  (pr query)
  ((get-method driver/execute-reducible-query :sql-jdbc) driver query chans respond))

(defmethod sql.qp/add-interval-honeysql-form :h2tcp
  [driver hsql-form amount unit]
  (cond
    (= unit :quarter)
    (recur driver hsql-form (hx/* amount 3) :month)

    ;; H2 only supports long ints in the `dateadd` amount field; since we want to support fractional seconds (at least
    ;; for application DB purposes) convert to `:millisecond`
    (and (= unit :second)
         (not (zero? (rem amount 1))))
    (recur driver hsql-form (* amount 1000.0) :millisecond)

    :else
    (hsql/call :dateadd (hx/literal unit) (long amount) hsql-form)))

(defmethod driver/humanize-connection-error-message :h2tcp
  [_ message]
  (condp re-matches message
    #"^A file path that is implicitly relative to the current working directory is not allowed in the database URL .*$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^Database .* not found .*$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^Wrong user name or password .*$"
    (driver.common/connection-error-messages :username-or-password-incorrect)

    #".*"                               ; default
    message))

(def ^:private date-format-str "yyyy-MM-dd HH:mm:ss.SSS zzz")

(defmethod driver.common/current-db-time-date-formatters :h2tcp
  [_]
  (driver.common/create-db-time-formatters date-format-str))

(defmethod driver.common/current-db-time-native-query :h2tcp
  [_]
  (format "select formatdatetime(current_timestamp(),'%s') AS VARCHAR" date-format-str))

(defmethod driver/current-db-time :h2tcp
  [& args]
  (apply driver.common/current-db-time args))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- add-to-1970 [expr unit-str]
  (hsql/call :timestampadd
    (hx/literal unit-str)
    expr
    (hsql/raw "timestamp '1970-01-01T00:00:00Z'")))

(defmethod sql.qp/unix-timestamp->honeysql [:h2tcp :seconds] [_ _ expr]
  (add-to-1970 expr "second"))

(defmethod sql.qp/unix-timestamp->honeysql [:h2tcp :millisecond] [_ _ expr]
  (add-to-1970 expr "millisecond"))


;; H2 doesn't have date_trunc() we fake it by formatting a date to an appropriate string
;; and then converting back to a date.
;; Format strings are the same as those of SimpleDateFormat.
(defn- format-datetime   [format-str expr] (hsql/call :formatdatetime expr (hx/literal format-str)))
(defn- parse-datetime    [format-str expr] (hsql/call :parsedatetime expr  (hx/literal format-str)))
(defn- trunc-with-format [format-str expr] (parse-datetime format-str (format-datetime format-str expr)))

(defmethod sql.qp/date [:h2tcp :minute]          [_ _ expr] (trunc-with-format "yyyyMMddHHmm" expr))
(defmethod sql.qp/date [:h2tcp :minute-of-hour]  [_ _ expr] (hx/minute expr))
(defmethod sql.qp/date [:h2tcp :hour]            [_ _ expr] (trunc-with-format "yyyyMMddHH" expr))
(defmethod sql.qp/date [:h2tcp :hour-of-day]     [_ _ expr] (hx/hour expr))
(defmethod sql.qp/date [:h2tcp :day]             [_ _ expr] (hx/->date expr))
(defmethod sql.qp/date [:h2tcp :day-of-week]     [_ _ expr] (hsql/call :day_of_week expr))
(defmethod sql.qp/date [:h2tcp :day-of-month]    [_ _ expr] (hsql/call :day_of_month expr))
(defmethod sql.qp/date [:h2tcp :day-of-year]     [_ _ expr] (hsql/call :day_of_year expr))
(defmethod sql.qp/date [:h2tcp :week]            [_ _ expr] (trunc-with-format "YYYYww" expr)) ; Y = week year; w = week in year
(defmethod sql.qp/date [:h2tcp :week-of-year]    [_ _ expr] (hx/week expr))
(defmethod sql.qp/date [:h2tcp :month]           [_ _ expr] (trunc-with-format "yyyyMM" expr))
(defmethod sql.qp/date [:h2tcp :month-of-year]   [_ _ expr] (hx/month expr))
(defmethod sql.qp/date [:h2tcp :quarter-of-year] [_ _ expr] (hx/quarter expr))
(defmethod sql.qp/date [:h2tcp :year]            [_ _ expr] (parse-datetime "yyyy" (hx/year expr)))

;; Rounding dates to quarters is a bit involved but still doable. Here's the plan:
;; *  extract the year and quarter from the date;
;; *  convert the quarter (1 - 4) to the corresponding starting month (1, 4, 7, or 10).
;;    (do this by multiplying by 3, giving us [3 6 9 12]. Then subtract 2 to get [1 4 7 10])
;; *  Concatenate the year and quarter start month together to create a yyyyMM date string;
;; *  Parse the string as a date. :sunglasses:
;;
;; Postgres DATE_TRUNC('quarter', x)
;; becomes  PARSEDATETIME(CONCAT(YEAR(x), ((QUARTER(x) * 3) - 2)), 'yyyyMM')
(defmethod sql.qp/date [:h2tcp :quarter]
  [_ _ expr]
  (parse-datetime "yyyyMM"
                  (hx/concat (hx/year expr) (hx/- (hx/* (hx/quarter expr)
                                                        3)
                                                  2))))

(defmethod sql.qp/->honeysql [:h2tcp :log]
  [driver [_ field]]
  (hsql/call :log10 (sql.qp/->honeysql driver field)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(def ^:private db-type->base-type
  {:ARRAY                               :type/*
   :BIGINT                              :type/BigInteger
   :BINARY                              :type/*
   :BIT                                 :type/Boolean
   :BLOB                                :type/*
   :BOOL                                :type/Boolean
   :BOOLEAN                             :type/Boolean
   :BYTEA                               :type/*
   :CHAR                                :type/Text
   :CHARACTER                           :type/Text
   :CLOB                                :type/Text
   :DATE                                :type/Date
   :DATETIME                            :type/DateTime
   :DEC                                 :type/Decimal
   :DECIMAL                             :type/Decimal
   :DOUBLE                              :type/Float
   :FLOAT                               :type/Float
   :FLOAT4                              :type/Float
   :FLOAT8                              :type/Float
   :GEOMETRY                            :type/*
   :IDENTITY                            :type/Integer
   :IMAGE                               :type/*
   :INT                                 :type/Integer
   :INT2                                :type/Integer
   :INT4                                :type/Integer
   :INT8                                :type/BigInteger
   :INTEGER                             :type/Integer
   :LONGBLOB                            :type/*
   :LONGTEXT                            :type/Text
   :LONGVARBINARY                       :type/*
   :LONGVARCHAR                         :type/Text
   :MEDIUMBLOB                          :type/*
   :MEDIUMINT                           :type/Integer
   :MEDIUMTEXT                          :type/Text
   :NCHAR                               :type/Text
   :NCLOB                               :type/Text
   :NTEXT                               :type/Text
   :NUMBER                              :type/Decimal
   :NUMERIC                             :type/Decimal
   :NVARCHAR                            :type/Text
   :NVARCHAR2                           :type/Text
   :OID                                 :type/*
   :OTHER                               :type/*
   :RAW                                 :type/*
   :REAL                                :type/Float
   :SIGNED                              :type/Integer
   :SMALLDATETIME                       :type/DateTime
   :SMALLINT                            :type/Integer
   :TEXT                                :type/Text
   :TIME                                :type/Time
   :TIMESTAMP                           :type/DateTime
   :TINYBLOB                            :type/*
   :TINYINT                             :type/Integer
   :TINYTEXT                            :type/Text
   :UUID                                :type/Text
   :VARBINARY                           :type/*
   :VARCHAR                             :type/Text
   :VARCHAR2                            :type/Text
   :VARCHAR_CASESENSITIVE               :type/Text
   :VARCHAR_IGNORECASE                  :type/Text
   :YEAR                                :type/Integer
   (keyword "DOUBLE PRECISION")         :type/Float
   :OBJECT                              :type/*             ;; @author Vincent Zhang ivincent.zhang@gmail.com 20200809
   (keyword "TIMESTAMP WITH TIME ZONE") :type/DateTimeWithLocalTZ})

(defmethod sql-jdbc.sync/database-type->base-type :h2tcp
  [_ database-type]
  (db-type->base-type database-type))

(defmethod sql-jdbc.conn/connection-details->spec :h2tcp
  [_ {ssl? :ssl, :as details-map}]
  (-> details-map
      (update :port (fn [port]
                      (if (string? port)
                        (Integer/parseInt port)
                        port)))
      ;; remove :ssl in case it's false; DB will still try (& fail) to connect if the key is there
      (dissoc :ssl)
      (set/rename-keys {:dbname :db})
      db.spec/h2tcp
      (sql-jdbc.common/handle-additional-options details-map)))

(defmethod sql-jdbc.sync/active-tables :h2tcp
  [& args]
  (apply sql-jdbc.sync/post-filtered-active-tables args))

(defmethod sql-jdbc.execute/connection-with-timezone :h2tcp
  [driver database ^String timezone-id]
  ;; h2 doesn't support setting timezones, or changing the transaction level without admin perms, so we can skip those
  ;; steps that are in the default impl
  (let [conn (.getConnection (sql-jdbc.execute/datasource database))]
    (try
      (doto conn
        (.setReadOnly false))
      (catch Throwable e
        (.close conn)
        (throw e)))))

;; de-CLOB any CLOB values that come back
(defmethod sql-jdbc.execute/read-column-thunk :h2tcp
  [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (let [classname (some-> (.getColumnClassName rsmeta i)
                          (Class/forName true (classloader/the-classloader)))]
    (if (isa? classname Clob)
      (fn []
        (jdbc-protocols/clob->str (.getObject rs i)))
      (fn []
        (.getObject rs i)))))

(defmethod sql-jdbc.execute/set-parameter [:h2tcp OffsetTime]
  [driver prepared-statement i t]
  (let [local-time (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))]
    (sql-jdbc.execute/set-parameter driver prepared-statement i local-time)))
