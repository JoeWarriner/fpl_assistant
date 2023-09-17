import type {Player} from '../types/player';
import React from 'react';


interface RowProps {
    player: Player
}

export function PlayerRow(props: RowProps){
    const player = props.player
    return (
            <tr>
                <td> {player.first_name} </td>
                <td> {player.second_name} </td>
                <td> {player.position} </td>
                <td> {player.team} </td>
                <td> {player.current_value} </td>
                <td> {player.predicted_score} </td>
            
            </tr>  
    );    
}

