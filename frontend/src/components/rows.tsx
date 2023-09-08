import type {Player} from '../types/player';
import React from 'react';


interface rowProps {
    first_name: string,
    second_name: string,
    position: string
}

export function PlayerRow(props: rowProps){
    return (
            <tr>
                <td> {props.first_name} </td>
                <td> {props.second_name} </td>
                <td> {props.position} </td>
            </tr>  
    );    
}

