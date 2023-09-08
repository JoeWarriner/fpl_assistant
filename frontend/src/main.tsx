import React, { Children } from 'react';
import ReactDOM from 'react-dom/client';
import 'bootstrap/dist/css/bootstrap.css';
import 'bootstrap/dist/js/bootstrap.min.js';


import { createBrowserRouter,  RouterProvider } from "react-router-dom";
import Home from './pages/Home';
import TeamSelector from './pages/TeamSelector';
import Trade from './pages/Trade';
import PlayerData from './pages/PlayerData';

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
      path: "player_data",
      element: <PlayerData />
    }
  ]
  },
  
]);

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router = {router} />
  </React.StrictMode>,
);
