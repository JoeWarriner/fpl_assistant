import type {Player} from '../types/player';
import {DataGetter} from  '../services/backend';
import {PlayerRow} from './rows'
import React, { useState, useEffect } from 'react';
import { PlayerPageViewer } from './page-viewers';


interface PlayerTableProps {
    offSet: number
    pageSize: number
    dataGetter: DataGetter
}

export function PlayerTable(props: PlayerTableProps){

    const [players, setPlayers] = useState<Player[]>([]);

    React.useEffect(() => {
        props.dataGetter(props.pageSize, props.offSet).then((result) => {
            setPlayers(result);
        }).catch((error) => {
            console.log("Error fetching data", error);
        });
    },[props]);

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
