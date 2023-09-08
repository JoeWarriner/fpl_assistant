import type {Player} from '../types/player';
import {getPlayers} from  '../services/backend';
import {PlayerRow} from './rows'
import React, { useState, useEffect } from 'react';


interface PagePosition {
    offSet: number
}

export function PlayerTable(props: PagePosition){

    const [players, setPlayers] = useState<Player[]>([]);

    React.useEffect(() => {
        getPlayers(10, props.offSet).then((result) => {
            setPlayers(result);
        }).catch((error) => {
            console.log("Error fetching data", error);
        });
    },[props.offSet]);

    return (
        <table>
            <th>
                <th> First Name </th> 
                <th> Second Name </th> 
                <th> Position </th> 
            </th>
            {players.map(
                (player) => <PlayerRow
                    key={player.id} 
                    first_name={player.first_name} 
                    second_name={player.second_name}
                    position={player.position} />
 
            )
            }
        </table>
    );
}
