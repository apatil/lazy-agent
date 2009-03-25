(load-file "lazy-agent.clj")
(refer 'lazy-agent :only ['def-cell 'update 'evaluate 'force-needs-update 'force-evaluate 'force-update])

(defn set-agent! [a v] (send a (fn [x] v)))

(defn sleeping [fun]
    (fn [& x] (do (Thread/sleep 1000) (apply fun x))))
;(defn sleeping [fun] fun)

(def x (ref 10))
(def-cell a (sleeping /) [1 x])
(def-cell b (sleeping +) [2 3])
(def-cell c (sleeping +) [a b] true)
(def-cell d (sleeping +) [c a 3])
(def-cell e (sleeping +) [a 2] true)
(def-cell f (sleeping +) [c e 12])


;(time (evaluate d e f))
;(set-agent! x 13)
;(time (evaluate d e f))
;
;(def-cell g (sleeping +) [a b c f])
;(time (evaluate g))
;
;
;(set-agent! x 12)
;(time (evaluate a b c d e f g))
;(time (force-evaluate a b c d e f g))
;(time (update a b c d e f g))
;(force-need-update a b c d e f g)
;(time (evaluate a b c d e f g))
;(time (update a b c d e f g))
;(time (evaluate a b c d e f g))