(ns netz.transport.secure
  "Assembly helpers for the authenticated Kabel/Netz stack.

  This namespace is optional until the Kabel transport-context and Geheimnis
  Noise releases land. Requiring it opts into those APIs; the ordinary Netz
  namespace graph continues to work with the last released dependencies."
  (:require [kabel.transport :as transport]
            [netz.overlay.runtime :as runtime]
            [netz.transport.noise :as noise]))

(defn middleware
  "Construct the Noise transport middleware installed in Kabel's final peer
  options map. This alias keeps application assembly on the Netz API surface."
  [options]
  (noise/middleware options))

(defn start!
  "Start an overlay that trusts the identity already authenticated by Noise.

  Unlike `netz.overlay.runtime/start!`, this never emits the transitional
  public hello/proof exchange. The peer MUST have `middleware` installed as
  its Kabel `:transport-middleware`; otherwise no authenticated context can
  arrive and the connection times out closed."
  [S peer options]
  (runtime/start! S peer
                  (assoc options
                         :authenticated-context-fn
                         transport/connection-context)))
