import {Player} from '../types/player';


export type DataGetter  = (pageSize: number, offSet: number) => Promise<Player[]>


export const getPlayers = async (pageSize: number, offSet: number): Promise<Player[]> => {
    const baseUrl = "http://127.0.0.1:8000/players";
    const requestUrl = baseUrl.concat('?', 'pagesize=', pageSize.toString(), '&', 'offset=', offSet.toString())
    try {
        const response = await fetch(requestUrl);
        const team: Player[] = await response.json() as Player[];
        return team;
    } catch (error)  {
        console.log('Error getting data from server: ', error);
        return [];
    }
}


export const getOptimalTeam = async (pageSize: number, offSet: number) => {
    try {
        const response = await fetch("http://127.0.0.1:8000/optimised-team");
        const team: Player[] = await response.json() as Player[];
        return team;
    } catch (error)  {
        console.log('Error getting data from server: ', error);
        return [];
    }
}