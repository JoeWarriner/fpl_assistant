import React, { Children } from 'react'
import ReactDOM from 'react-dom/client'

import { createBrowserRouter,  RouterProvider } from "react-router-dom";
import Home from './pages/Home';
import TeamSelector from './pages/TeamSelector';

const router = createBrowserRouter([
  {
    path: "/",
    element: <Home />,
  },
  {
    path: "team-selector",
    element: <TeamSelector />
  }
]);

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router = {router} />
  </React.StrictMode>,
)
