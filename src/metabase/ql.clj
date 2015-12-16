(ns metabase.ql
  (:refer-clojure :exclude [< <= > >= = != not and or nil? filter count sum])
  (:require [clojure.core :as core]
            [schema.core :as s]))

(def Table s/Int)

(def Field {(s/required-key :field-id) s/Int})

(def FieldOrNumber (s/named (s/cond-pre s/Num Field) "Field or numerical value"))
(def FieldOrString (s/named (s/cond-pre s/Str Field) "Field or string"))


(def Value (s/named (s/cond-pre s/Str s/Num s/Bool (s/eq nil)) "Value"))

(def FieldOrValue (s/named (s/cond-pre Value Field) "FieldOrValue"))

(def OneOrMoreFields [(s/one Field "f") Field])


;;; ## ag

(def FieldlessAggregationType (s/enum :count))

(def FieldlessAggregation
  {(s/required-key :aggregation-type) FieldlessAggregationType})

(def FieldAggregationType (s/enum :average :field-count :field-distinct-count :standard-deviation :sum :cumulative-sum :minimum :maximum :median :mode))

(def FieldAggregation
  {(s/required-key :aggregation-type) FieldAggregationType
   (s/required-key :field)            Field})

(def Aggregation
  (s/if :field
    FieldAggregation
    FieldlessAggregation))


;;; ## sort

(def Sort
  (s/if keyword?
    (s/eq :random)
    {(s/required-key :field)     Field
     (s/required-key :direction) (s/enum :ascending :descending)}))


;;; ## filter

;;; filter

(def BetweenFilter
  {(s/required-key :filter-type) (s/eq :between?)
   (s/required-key :value)       FieldOrNumber
   (s/required-key :min)         FieldOrNumber
   (s/required-key :max)         FieldOrNumber})

(def StringFilter
  {(s/required-key :filter-type)     (s/enum :starts-with? :ends-with? :contains-str? :matches-regex?)
   (s/required-key :value)           FieldOrString
   (s/required-key :string)          FieldOrString
   ;; defaults to `false` (when applicable -- not available on all DBs)
   (s/optional-key :case-sensitive?) s/Bool})

(def NilFilter
  {(s/required-key :filter-type) (s/enum :nil? :not-nil?)
   (s/required-key :value)       FieldOrValue})

;; couldn't these technically support >2 args ?
(def ComparisonFilter
  {(s/required-key :filter-type) (s/enum :< :<= :> :>=)
   (s/required-key :values)      [(s/one FieldOrNumber "Field or numerical value in values[0]")
                                  (s/one FieldOrNumber "Field or numerical value in values[1]")]})

(def EqualityFilter
  {(s/required-key :filter-type) (s/enum := :!)
   (s/required-key :values)      [(s/one FieldOrValue "Field or value in values[0]")
                                  (s/one FieldOrValue "Field or value in values[1]")]})

(def AnyFilter
  {(s/required-key :filter-type) (s/enum :matches-any? :matches-none?)
   (s/required-key :value)       FieldOrValue
   (s/required-key :in)          [FieldOrValue]})

(declare Filter)

(def NotFilter
  {(s/required-key :filter-type) (s/eq :not)
   (s/required-key :filter)      Filter})

(def CompoundFilter
  {(s/required-key :filter-type) (s/enum :and :or)
   (s/required-key :filters)     [(s/one Filter "f1") (s/one Filter "f2") Filter]})

(def Filter
  (let [type= (fn [& ks] (comp (partial contains? (set ks)) :filter-type))]
    (s/conditional
     (type= :between?)                      BetweenFilter
     (type= :starts-with? :ends-with?
            :contains-str? :matches-regex?) StringFilter
     (type= :nil? :not-nil?)                NilFilter
     (type= :< :<= :> :>=)                  ComparisonFilter
     (type= := :!=)                         EqualityFilter
     (type= :matches-any? :matches-none?)   AnyFilter
     ;; (type= :not)                           NotFilter
     ;; (type= :and :or)                       CompoundFilter
     )))


;;; ## TODO sort
;;; ## TODO limit
;;; ## TODO offset


;;; ## query

(def Query
  {(s/optional-key :version)      s/Num
   (s/required-key :table-id)     Table
   (s/optional-key :aggregations) [Aggregation]
   (s/optional-key :breakouts)    OneOrMoreFields
   (s/optional-key :fields)       [(s/one FieldOrValue "v1") FieldOrValue]
   (s/optional-key :filter)       Filter
   (s/optional-key :sort)         OneOrMoreFields
   ;; :sort
   ;; :limit
   ;; :offset
   })

(s/defn ^:always-validate query* :- Query [table :- Table]
  {:version  2.0
   :table-id table})

(defmacro query
  {:style/indent 1}
  [table & body]
  `(s/validate Query (-> (query* ~table)
                         ~@body)))

(defn- update-conj [m k & vs]
  (update m k (comp vec concat) vs))


;;; ## aggregate

(s/defn ^:always-validate count                :- FieldlessAggregation [] {:field field, :aggregation-type :count}) ; why generate this with a fn?

(s/defn ^:always-validate average              :- FieldAggregation [field :- Field] {:field field, :aggregation-type :average})
(s/defn ^:always-validate field-count          :- FieldAggregation [field :- Field] {:field field, :aggregation-type :field-count})
(s/defn ^:always-validate field-distinct-count :- FieldAggregation [field :- Field] {:field field, :aggregation-type :field-distinct-count})
(s/defn ^:always-validate standard-deviation   :- FieldAggregation [field :- Field] {:field field, :aggregation-type :standard-deviation})
(s/defn ^:always-validate sum                  :- FieldAggregation [field :- Field] {:field field, :aggregation-type :sum})
(s/defn ^:always-validate cumulative-sum       :- FieldAggregation [field :- Field] {:field field, :aggregation-type :cumulative-sum})
(s/defn ^:always-validate minimum              :- FieldAggregation [field :- Field] {:field field, :aggregation-type :minimum})
(s/defn ^:always-validate maximum              :- FieldAggregation [field :- Field] {:field field, :aggregation-type :maximum})
(s/defn ^:always-validate median               :- FieldAggregation [field :- Field] {:field field, :aggregation-type :median})
(s/defn ^:always-validate mode                 :- FieldAggregation [field :- Field] {:field field, :aggregation-type :mode})

(s/defn ^:always-validate aggregations :- Query [query :- Query, & aggregations :- [Aggregation]]
  (assoc query :aggregations (vec aggregations)))


;;; ## breakout

(s/defn ^:always-validate breakout :- Query [query :- Query, & fields :- [Field]]
  (apply update-conj query :breakouts fields))


;;; ## field

(s/defn ^:always-validate field :- Field [field-id :- s/Int]
  {:field-id 300})


;;; ## filter

(s/defn ^:always-validate between? :- BetweenFilter [v :- FieldOrNumber, min :- FieldOrNumber, max :- FieldOrNumber] {:filter-type :between?, :value v, :min min, :max max})

(s/defn ^:always-validate starts-with?  :- StringFilter [v :- FieldOrString, s :- FieldOrString, & [case-sensitive? :- (s/maybe s/Bool)]] {:value v, :string s, :filter-type :starts-with?})
(s/defn ^:always-validate ends-with?    :- StringFilter [v :- FieldOrString, s :- FieldOrString, & [case-sensitive? :- (s/maybe s/Bool)]] {:value v, :string s, :filter-type :ends-with?})
(s/defn ^:always-validate contains-str? :- StringFilter [v :- FieldOrString, s :- FieldOrString, & [case-sensitive? :- (s/maybe s/Bool)]] {:value v, :string s, :filter-type :contains-str?})

(s/defn ^:always-validate nil?     :- NilFilter [v :- FieldOrValue] {:value v, :filter-type :nil?})
(s/defn ^:always-validate not-nil? :- NilFilter [v :- FieldOrValue] {:value v, :filter-type :not-nil?})

(s/defn ^:always-validate <  :- ComparisonFilter [x :- FieldOrNumber, y :- FieldOrNumber] {:values [x y], :filter-type :<})
(s/defn ^:always-validate <= :- ComparisonFilter [x :- FieldOrNumber, y :- FieldOrNumber] {:values [x y], :filter-type :<=})
(s/defn ^:always-validate >  :- ComparisonFilter [x :- FieldOrNumber, y :- FieldOrNumber] {:values [x y], :filter-type :>})
(s/defn ^:always-validate >= :- ComparisonFilter [x :- FieldOrNumber, y :- FieldOrNumber] {:values [x y], :filter-type :>=})

(s/defn ^:always-validate =  :- EqualityFilter [x :- FieldOrValue, y :- FieldOrValue] {:values [x y], :filter-type :=})
(s/defn ^:always-validate != :- EqualityFilter [x :- FieldOrValue, y :- FieldOrValue] {:values [x y], :filter-type :!=})

(s/defn ^:always-validate matches-any?  :- AnyFilter [x :- FieldOrValue, & in :- [FieldOrValue]] {:value x, :in in, :filter-type :matches-any?})
(s/defn ^:always-validate matches-none? :- AnyFilter [x :- FieldOrValue, & in :- [FieldOrValue]] {:value x, :in in, :filter-type :matches-none?})

(s/defn ^:always-validate not :- NotFilter [filter :- Filter] {:filter-type :not, :filter filter})

(s/defn ^:always-validate and :- CompoundFilter [& filters :- [Filter]] {:filters filters, :filter-type :and})
(s/defn ^:always-validate or  :- CompoundFilter [& filters :- [Filter]] {:filters filters, :filter-type :or})

(s/defn ^:always-validate filter :- Query [query :- Query, & filters :- [Filter]]
  ;; TODO - handle muliple filters: automatically wrap in an AND clause
  ;; TODO - What if we already have a :filter in the query? Can we merge into it?
  (assoc query :filter (first filters)))


;;; TODO - limit
;;; TODO - sort
;;; TODO - offset
;;; TODO - constraints


(defn x []
  (query 100
    (aggregations (minimum (field 300)))
    (breakout (field 5))
    (filter (= (field 5) "abc"))))

;;; etc
;; TODO - 'nested' query-info ?
{:query-type :rows,            :fields []}
{:query-type :aggregation,     :aggregations []}
{:query-type :distinct-values, :fields []}
{:query-type :breakout,        :fields [], :aggregations []}







;;; # ------------------------------------------------------------ TRANSLATION ------------------------------------------------------------

(defn- normalize-token [token]
  {:pre [(core/or (string? token) (keyword? token))]}
  (-> (name token)
      clojure.string/lower-case
      (clojure.string/replace #"_" "-")
      keyword))

(defn- translate-aggregation
  ([query ag-type]
   (when (= ag-type :count)
     (aggregations query (count))))
  ([query ag-type field-id]
   (let [field (field field-id)]
     (aggregations query (case (normalize-token ag-type)
                           :count    (field-count field)
                           :avg      (average field)
                           :distinct (field-distinct-count field) ; TODO - these names *are* too long, switch back to the old names
                           :stddev   (standard-deviation field)
                           :sum      (sum field)
                           :cum-sum  (cumulative-sum field))))))

(def k->translator
  {:aggregation translate-aggregation})

(defn- translate [{{table-id :source_table, :as q} :query}]
  (loop [query (query table-id), [[k v] & more] (seq (dissoc q :source_table))]
    (let [k          (normalize-token k)
          args       (if (seq v) v [v])
          translator (core/or (k->translator k)
                              (throw (Exception. (format "No translator %s" (vec (concat [k] args))))))
          query      (core/or (apply translator query args)
                              query)]
      (if (seq more)
        (recur query more)
        query))))

(def q {:database    1
        :type        "query"
        :query       {:source_table 2
                      :aggregation  ["avg" 16]
                      ;; :breakout     []
                      ;; :filter       []
                      },
        :constraints {:max-results 10000, :max-results-bare-rows 2000}})

(defn- y [] (translate q))
