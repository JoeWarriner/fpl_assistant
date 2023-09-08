import {Player} from '../types/player';




export const getPlayers = async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/players");
        const team: Player[] = await response.json() as Player[];
        return team;
    } catch (error)  {
        console.log('Error getting data from server: ', error);
        return [];
    }
}


export const getOptimalTeam = async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/optimal-team");
        const team: Player[] = await response.json() as Player[];
        return team;
    } catch (error)  {
        console.log('Error getting data from server: ', error);
        return [];
    }
}