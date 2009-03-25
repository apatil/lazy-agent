; Author: Anand Patil
; Date: Feb 5, 2009
; Creative Commons BY-SA, see LICENSE
; copyright 2009 Anand Patil

; Good reference on scheduling: Scheduling and Automatic Parallelization.
; Chapter 1 covers scheduling in DAGs and is available free on Google Books.

(set! *warn-on-reflection* true)

; ==================================================
; = Utility stuff not immediately related to cells =
; ==================================================
;(ns lazy-agent)

(defmacro structmap-and-accessors [sym & fields]
    "Defunes a structmap with given symbol, and defines accessors for all its fields."
    (let [code-lst `(defstruct ~sym ~@fields)
            sym-dash (.concat (name sym) "-")
            accessor-names (zipmap fields (map (comp #(.concat sym-dash %) name) fields))]
        (cons 'do (cons code-lst
            (for [field fields] (let [n (accessor-names field) s (symbol n)]
                `(def ~s (accessor ~sym ~field))))))))
                
(defn agent? [x] (instance? clojure.lang.Agent x))
(defn id? [x] (instance? clojure.lang.IDeref x))
(defn deref-or-val [x] (if (id? x) @x x))
(defn map-now [fn coll] (dorun (map fn coll)))
(defn map-now-over-first [fun coll & others]
    (map-now #(apply fun (cons % others)) coll))
(defn map-now-with-first [fun coll & others]
    (map-now #(apply fun (cons % (cons % others))) coll))
(defn second-arg [x y] y)

; ===============================================================
; = Structmaps and accessors for cell values and cell metadata. =
; ===============================================================
(structmap-and-accessors cv 
    :val            ; Actual value of cell.
    :status)        ; Status of cell.    
(def needs-update-val (struct cv nil :needs-update))

(structmap-and-accessors cm 
    :la-parents     ; Set of parents that are lazy agents.
    :id-parent-vals ; Map from id parents to their values.
    :n-id-parents   ; Size of id parents set.
    :parents        ; Vector of parents.
    :children       ; Set of children.
    :fn             ; Function the cell uses to update its value.
    :oblivious?     ; Whether the function should update to :oblivious or :up-to-date
    :lazy-agent)    ; Tags the cell as a lazy agent.

; =========================================
; = Utility functions for examining cells =
; =========================================
(def deref-cell (comp cv-val deref))
(defn is-lazy-agent? [x] (-> x deref meta :lazy-agent))
(defn up-to-date? [cell] (= :up-to-date (cv-status cell)))
(defn inherently-oblivious? [cv] (-> cv meta cm-oblivious?))
(defn oblivious? [cell] (= :oblivious (cv-status cell)))
(defn updating? [cell] (= :updating (cv-status cell)))
(defn error? [cell] (= :error (cv-status cell)))
(defn needs-update? [cell] (= :needs-update (cv-status cell)))


; =========================
; = Messages for updating =
; =========================        
(defn complete-parents [parent-v-map parents]
    "Takes a map of the form {parent @parent}, and a list of mutable and
    immutable parents, and returns a list of the parents' vs in the
    correct order."
    (loop [parents-sofar parents v-sofar (list)]
        (if (empty? parents-sofar) v-sofar
            (let [parent (last parents-sofar) 
                rest-parents (butlast parents-sofar)]
                (if (id? parent)
                    ; If v has a key corresponding to this parent, cons the corresponding v
                    (recur rest-parents (cons (parent-v-map parent) v-sofar))
                    ; Otherwise, cons the parent.
                    (recur rest-parents (cons parent v-sofar)))))))
        
(defn record-la-parent-v [m pv-map p pv]
    "Utility function that incorporates updated ps into a cell's
    p v ref." 
    (assoc m :id-parent-vals (assoc pv-map p (cv-val pv))))

(defn forget-la-parent-v [m pv-map p]
    "Utility function that incorporates updated ps into a cell's
    p v ref." 
    (assoc m :id-parent-vals (dissoc pv-map p)))

(defn update-m [v p new-pv] 
    "Message sent when a parent computes."
    (let [old-m (meta v)
        old-pv-map (cm-id-parent-vals old-m)
        m (record-la-parent-v old-m old-pv-map p new-pv)
        new-v (with-meta v m)
        pv-map (cm-id-parent-vals m)] 
        ; If oblivious, do nothing
        (if (oblivious? new-v) 
            new-v
            ; If updating, try to compute
            (if (updating? new-v)
                (if (= (cm-n-id-parents m) (count pv-map))
                    ((cm-fn m) new-v m pv-map)
                    new-v)
                ; Otherwise do nothing.
                new-v))))
            
(defn update-request-m [v c]
    "Message sent to put a cell in the updating state."
    ; If the cell is not in the needs-update state, do nothing.
    (if (needs-update? v)
        (let [m (meta v)
                pv-map (cm-id-parent-vals m)]
            (if (= (count pv-map) (cm-n-id-parents m))
                ; Compute if possible
                ((cm-fn m) v m pv-map)
                (do
                    ; Propagate update request to parents.
                    (map-now #(send % update-request-m %) (cm-la-parents (meta v)))                
                    ; Otherwise put cell into the updating status.
                    (assoc v :status :updating))))
        v))
    
(defn error-m [v p err]
    "Message sent when a parent enters the error state."
    ; Drop the parent from the parent value map if necessary.
    (let [old-m (meta v)
            old-pv-map (cm-id-parent-vals old-m)
            m old-m
            m (forget-la-parent-v old-m old-pv-map p)
            new-v (with-meta v m)]
        ; If the cell is oblivious, do nothing.
        (if (oblivious? new-v) 
            new-v
            (do
                ; If the cell is non-oblivious, first propagate the error message to its children,
                (map-now #(send % error-m p err) (cm-children m))
                (if (error? new-v)
                    ; If the error is already known, do nothing.
                    (if (= ((cv-val new-v) p) err)
                        new-v
                        ; If the cell is non-oblivious, either set its status to error or if it is already
                        ; error incorporate the new parent value.
                        (assoc new-v :val (assoc (cv-val new-v) p err)))
                (with-meta (struct cv {p err} :error) m))))))
            
(defn recovery-m [v p]
    "Message sent when a parent goes from the error state to the needs-update state."
    ; If the cell is not in an error state (if it is oblivious or the recovery 
    ; has already been noted), do nothing.
    (if (not (error? v)) 
        v
        (let [new-v (assoc v :val (dissoc (cv-val v) p)) 
                m (meta v)]
            (if (not ((cv-val v) p))
                ; If the recovery has already been noted, do nothing.
                new-v
                (do
                    ; Propagate the recovery message to children.
                    (map-now #(send % recovery-m p) (cm-children m))
                    ; If no ancestral errors remain, leave the error state.
                    (if (empty? (cv-val new-v)) 
                        (with-meta needs-update-val m) 
                        new-v))))))

(defn needs-update-m [v c p]
    "Message sent when a parent goes into the needs-update state."
    ; Drop the parent from the parent value map if necessary.
    (let [old-m (meta v)
        old-pv-map (cm-id-parent-vals old-m)
        m (forget-la-parent-v old-m old-pv-map p)
        new-v (with-meta v m)]
        ; If the cell is not up-to-date (oblivious, updating, needing an update, or in error)
        ; do nothing.
        (if (not (up-to-date? new-v)) 
            new-v
            (do
                ; First propagate the needs-update message to its children,
                (map-now #(send % needs-update-m % c) (cm-children m))
                ; Then set the cell's status to needs-update
                (with-meta needs-update-val m)))))
                

; ================================================
; = Updating functions to be called by the user. =
; ================================================

;(defn set-cell! [c v] 
;    "Sets a cell's val to v, and sets its status to either :updated or :oblivious as appropriate."
;    (send c 
;        (fn [old-v] (let [updated-status (if (inherently-oblivious? old-v) :oblivious :up-to-date)
;                            old-meta (meta old-v)
;                            children (cm-children old-meta)
;                            new-v (with-meta (struct cv v updated-status) old-meta)]
;            (do 
;                ; If switching from error state, send recovery message
;                (if (error? old-v) (map-now-over-first recovery-m children c))
;                ; Send compute message
;                (map-now-with-first update-m children c v)
;                new-v)))))

(defn force-needs-update [c] "Utility function that puts p into the needs-update state, even if p is oblivious." 
    (send c 
        (fn [v] (let [m (meta v)
                        new-v (with-meta needs-update-val m)
                        children (cm-children m)]
            (do
                (map-now (if (error? v) #(send % recovery-m c) #(send % needs-update-m % c))
                    children)
                new-v)))))

(defn update [c] "Utility function that puts p into the updating state if it needs an update." 
    (send c #(update-request-m % c)))

(defn force-error [c] "Puts the cell into the error state." 
    (send c 
        (fn [v] (let [m (meta v)
                        new-v (with-meta (struct cv {:self (Error.)} :error) m)
                        children (cm-children m)
                        err (-> new-v cv-val :self)]
            (do
                (map-now #(send % error-m c err) children) 
                new-v)))))


; =======================
; = Cell creation stuff =
; =======================

(defn updated? [c] 
    (let [status (-> c deref :status)]
        (if (not status) 
            true 
            (= status :up-to-date))))
(defn extract-val [x] (let [v (:val x)] (if v v x)))
(def extract-cv (comp extract-val deref))
(defn add-child-msg [v c]
    (let [old-meta (meta v)
        children (cm-children old-meta)
        new-children (conj children c)
        new-meta (assoc old-meta :children new-children)]
        (with-meta v new-meta)))

(defn update-wrap [c update-fn new-status]
    "Wraps a cell's update function, closing it on the cell itself and its update function.
    Does not close on parents or children to make it easier to change those later."
    (fn [v m id-parent-vals]
        (let [parents (cm-parents m)
            new-parents (complete-parents id-parent-vals parents)
            children (cm-children m)
            new-v (struct cv (apply update-fn new-parents) new-status)] 
            (do
                ; Send compute message to children.
                (map-now #(send % update-m c new-v) children)
                ; Create new v, preserving metadata, and put cell in either up-to-date or oblivious state.
                (with-meta new-v m)))))
        
(defn cell [name update-fn parents & [oblivious?]]
    "Creates a cell (lazy auto-agent) with given update-fn and parents."
    (let [parents (vec parents)
        id-parents (set (filter id? parents))
        non-la-parents (filter (comp not is-lazy-agent?) id-parents)
        la-parents (filter is-lazy-agent? id-parents) 
        n-id-parents (count id-parents)
        updated-parents (filter updated? id-parents)          
        id-parent-vals (zipmap updated-parents (map extract-cv updated-parents))
        cell (agent (with-meta
            needs-update-val
                (struct cm la-parents id-parent-vals n-id-parents parents #{} nil oblivious? true)))]
        (do
            (map-now #(send % add-child-msg cell) la-parents)
            (send cell 
                #(with-meta % 
                    (assoc (meta %) :fn (update-wrap cell update-fn (if oblivious? :oblivious :up-to-date))))))))        

(defmacro def-cell
    "Creates and inters a cell in the current namespace, bound to sym,
    with given parents and update function."
    [sym update-fn parents & [oblivious?]] 
    `(def ~sym (cell ~(name sym) ~update-fn ~parents ~oblivious?)))

;; =======================================
;; = Synchronized multi-cell evaluations =
;; =======================================
;(defn not-waiting? [cv] 
;    "Determines whether a cell is either up-to-date or oblivious."
;    (let [status (cv-status cv)]
;        (or 
;            (= :up-to-date status) 
;            (= :oblivious status))))
;
;(defn unlatching-watcher [#^java.util.concurrent.CountDownLatch latch cell old-val new-val]
;    "A watcher function that decrements a latch when a cell updates."
;    (do
;        (if (not= old-val new-val)
;            (if (not-waiting? new-val)
;                (.countDown latch)))
;            latch))
;
;(def cell-waiting? (comp not not-waiting? deref))
;(defn evaluate [& cells]
;    "Updates the cells, waits for them to compute, and returns their vals."
;    (let [        
;          latch (java.util.concurrent.CountDownLatch. (count (filter cell-waiting? cells)))
;          watcher-adder (fn [cell] (add-watch cell latch unlatching-watcher))
;          watcher-remover (fn [cell] (remove-watch cell latch))]
;        (do
;            (map-now watcher-adder cells)            
;            (apply update cells)             
;            (.await latch)
;            (map-now watcher-remover cells)
;            (map deref-cell cells))))

;(defn force-update [& cells]
;    "Forces the cells to update and returns them immediately."
;    (do
;        (apply force-need-update cells)
;        (apply await cells)
;        (apply update cells)))
                
;(defn force-evaluate [& cells]
;    "Forces the cells to update, waits for them and returns their vals."
;    (do 
;        (apply force-need-update cells)
;        (apply await cells)
;        (apply evaluate cells)))

;; ============================
;; = Change cell dependencies =
;; ============================
;
;(defn conditional-map-replace [old-map old-key new-key dissoc-cond assoc-cond]
;    "Utility function for switching parents."
;    (let [dissoc-map (if dissoc-cond (dissoc old-map old-key) old-map)
;            val (deref-or-val new-key)]
;        (if assoc-cond 
;            (if (up-to-date? val) (assoc dissoc-map new-key (extract-val val)) dissoc-map)
;            dissoc-map)))
;        
;(defn conditional-set-replace [old-set old-val new-val disj-cond conj-cond]
;    "Utility function for switching parents."
;    (let [disj-set (if disj-cond (disj old-set old-val) old-set)]
;        (if conj-cond (conj disj-set new-val) disj-set)))
;        
;(defn conditional-counter-change [old-ctr dec? inc?]
;    "Utility function for switching parents."    
;    (let [dec-ctr (if dec? (- old-ctr 1) old-ctr)]
;        (if inc? (+ dec-ctr 1) dec-ctr)))
;
;(defn replace-parent-msg [cv old-parent new-parent]
;    "This message is sent to cells by replace-parent."
;    (let [old-meta (meta cv)
;        parents (replace {old-parent new-parent} (cm-parents old-meta))
;        new-id? (id? new-parent)
;        old-id? (id? old-parent)
;        new-agent? (agent? new-parent)
;        old-agent? (agent? old-parent)
;        agent-parents (conditional-set-replace (cm-agent-parents old-meta) old-parent new-parent old-agent? new-agent?)
;        id-parent-vals (conditional-map-replace (cm-id-parent-vals old-meta) old-parent new-parent old-id? new-id?)
;        n-id-parents (conditional-counter-change (cm-n-id-parents old-meta) old-id? new-id?)]
;    (with-meta (if (= (cv-status cv) :oblivious) cv needs-update-val)
;        (assoc old-meta 
;            :parents parents
;            :agent-parents agent-parents 
;            :id-parent-vals id-parent-vals 
;            :n-id-parents n-id-parents))))
;            
;(defn replace-parent [cell old-parent new-parent]
;    "Replaces a cell's parent. Sets the cell's val to needs-update, or leaves
;    it unchanged if the cell is oblivious."
;    (do 
;        ; Remove old watcher
;        (remove-watch old-parent cell)                                                            
;        ; Tell the cell to update its metadata and go into the needs-update state if not oblivious.
;        (send cell replace-parent-msg old-parent new-parent)                      
;        ; Add new watcher.                
;        (add-watch new-parent cell 
;            (watcher-to-watch 
;                (let [oblivious? (-> cell deref meta cm-oblivious?)]
;                    (if (is-lazy-agent? new-parent) 
;                    (parent-watcher oblivious?) 
;                    (report-to-child false oblivious?)))))))

;(defn reaction [val meta]
;    ; If the child is not oblivious, put it in the needs-update state.
;    (with-meta (if (needs-update? val) val needs-update-val) meta))
;
;(defn report-to-child [parent-lazy-agent? oblivious?]
;    "Called by parent-watcher when a parent either updates or reverts to
;    the 'needs-update' state. If a parent updates and the child cell wants
;    to update, computation is performed if possible. If a parent reverts
;    to the needs-update state, the child is put into the needs-update 
;    state also."
;    (let [swap-fn (if parent-lazy-agent? swap-la-parent-val swap-id-parent-val)
;            react-fn (if oblivious? with-meta reaction)
;            updated-status (if oblivious? :oblivious :up-to-date)]
;        (fn [cur-val parent old-val parent-val]
;            (let [cur-meta (meta cur-val)
;                new-id-parent-vals (swap-fn (cm-id-parent-vals cur-meta) parent parent-val)
;                new-meta (assoc cur-meta :id-parent-vals new-id-parent-vals)] 
;
;            
;            (if
;                ; If the child is updating, check whether it's ready to compute.
;                (updating? cur-val) (if (= (count new-id-parent-vals) (cm-n-id-parents new-meta))
;                    (compute-cv cur-val new-meta new-id-parent-vals updated-status)
;                    (with-meta cur-val new-meta))
;                
;                ; React to the new val.
;                (react-fn cur-val new-meta))))))
;
;(defn report-recovery [cur-val key]
;    "Acknowledges a recovery in an ancestor."
;    (let [new-val (dissoc cur-val key)]
;        (if (empty? new-val) 
;            (with-meta needs-update-val (meta cur-val)) 
;            new-val)))
;
;(defn report-error [cur-val key err]
;    "Acknowledges an error in an ancestor."
;    (if (error? cur-val)
;        (assoc cur-val :val (assoc (cv-val cur-val) key err))
;        (with-meta (struct cv {key err} :error) (meta cur-val))))
;
;(defn watcher-to-watch [fun]
;    "Converts an 'old-style' watcher function to a new synchronous watch.
;    The watcher is used as the key of the new watch."
;    (fn [watcher reference old-val new-val]
;        (if (not= old-val new-val)
;            (send watcher fun reference old-val new-val))))        
;
;(defn parent-watcher [oblivious?]
;    "Watches a parent cell on behalf of one of its children. This watcher 
;    has access to a ref which holds the vals of all the target child's 
;    updated parents. It also reports parent chages to the child."
;    (let [report (report-to-child true oblivious?)]
;        (fn [cur-val p old-val p-val]
;            (cond 
;                (error? old-val)
;                    (not (error? p-val)
;                        (report-recovery cur-val p))
;                (error? p-val) 
;                    (report-error cur-val p p-val)                    
;                (not (updating? p-val)) 
;                    (report cur-val p p-val)
;                true cur-val))))
;
;(defn cell-watch [updated-status cell old-val cv]
;    "Watches a non-root cell. If it changes and requests an update,
;    it computes if possible. Otherwise it sends an update request to all its
;    parents."
;    (if (not= old-val cv)
;        (if (updating? cv)
;            (let [cm (meta cv)
;                    id-parent-vals (cm-id-parent-vals cm)
;                    num-id-parent-vals (count id-parent-vals)
;                    num-id-parents (cm-n-id-parents cm)]
;                ; If the cell has changed into the updating state, check whether an immediate computation is possible.
;                (if (=  num-id-parent-vals num-id-parents)
;                    ; Compute if possible
;                    (send cell compute-cv cm id-parent-vals updated-status)
;                    ; Otherwise put all parents that need updates into the updating state.
;                    (map-now send-update (cm-agent-parents cm)))))))
