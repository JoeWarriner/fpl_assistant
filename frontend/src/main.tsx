import React, { Children } from 'react'
import ReactDOM from 'react-dom/client'
import 'bootstrap/dist/css/bootstrap.css'
import 'bootstrap/dist/js/bootstrap.min.js';


import { createBrowserRouter,  RouterProvider } from "react-router-dom";
import Home from './pages/Home';
import TeamSelector from './pages/TeamSelector';
import Trade from './pages/Trade';
import Visualiser from './pages/Visualiser';

const router = createBrowserRouter([
  {
    path: "/",
    element: <Home />,
    children: [
    {
      path: "team-selector",
      element: <TeamSelector />
    },
    {
      path: "trade",
      element: <Trade />
    },
    {
      path: "visualiser",
      element: <Visualiser />
    }
  ]
  },
  
]);

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router = {router} />
  </React.StrictMode>,
)
