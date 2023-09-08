import React, { useState, useEffect } from 'react';
import { getPlayers } from '../services/backend';
import {Player } from '../types/player';
import { PlayerRow } from '../components/players';

const PlayerData = () => {
    
    const [players, setPlayers] = useState<Player[]>([]);


    useEffect(() => {
        getPlayers().then((result) => {
            setPlayers(result);
        }).catch((error) => {
            console.log("Error fetching data", error);
        });
    },[]);

    return (
        <table>
            {players.map((player) => PlayerRow(player))}
        </table>
    )
    
}

export default PlayerData;
