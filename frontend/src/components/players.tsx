import type {Player} from '../types/player';
import React from 'react';


export function PlayerRow(player: Player): JSX.Element{
    return (
            <tr>
                <td> {player.first_name} </td>
                <td> {player.second_name} </td>
                <td> {player.position} </td>
            </tr>
        
    );
    
}
