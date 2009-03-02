(load-file "lazy-agent.clj")
(refer 'lazy-agent :only ['def-cell 'update 'evaluate 'set-agent! 'force-need-update])

;(defn sleeping [fun]
;    (fn [& x] (do (Thread/sleep 1000) (apply fun x))))
(defn sleeping [fun] fun)

(def x (agent 10))
(def-cell a (sleeping +) [1 x])
(def-cell b (sleeping +) [2 3])
(def-cell c (sleeping +) [a b] true)
(def-cell d (sleeping +) [c a 3])
(def-cell e (sleeping +) [a 2] true)
(def-cell f (sleeping +) [c e 12])

;(time (evaluate d e f))
;(set-agent! x 13)
;(time (evaluate d e f))
;
;(set-agent! x 12)
;(time (evaluate d e f))
;(set-agent! x 213)
;(time (evaluate a b c d e f))
;(force-need-update a b c d e f)
;(time (evaluate a b c d e f))
;(force-need-update a b c d e f)
;(time (evaluate a b c d e f))
;;(time (evaluate a b c d e f))