import React, { useState, useEffect } from 'react';

import type {Player} from '../types/player';
import {PlayerRow } from '../components/rows';
import { getOptimalTeam } from '../services/backend';




const TeamSelector = () => {

    const [team, setTeam] = useState<Player[]>([]);
    
    useEffect(() => {
        getOptimalTeam().then((result) => {
            setTeam(result);
         }).catch((error) => {
            console.log("Error fetching data", error);
         });
    }, []);

    
    return (
        <>
            <p>This is the team selector page. The best team is: </p>
            <table>
            {team.map((player) => PlayerRow(player) )}
            </table>
        </>
    );
}

export default TeamSelector;