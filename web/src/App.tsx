import { Routes, Route } from "react-router-dom";
import { NetworkFulfillmentScreen } from "./screens/NetworkFulfillmentScreen";

/** Exposed as netfulfil_mfe/App via Module Federation. Routed under
 *  /network-fulfillment/* by the shell -- uses relative routes so this
 *  component works identically mounted under a prefix (in the shell) or
 *  at / (standalone dev, see main.tsx). */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<NetworkFulfillmentScreen />} />
    </Routes>
  );
}
