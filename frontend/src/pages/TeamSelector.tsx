import React, { useState, useEffect } from 'react';

const getTeamData = async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/best-team");
        const team: string[] = await response.json() as string[];
        return team;
    } catch (error)  {
        console.log('Error getting data from server: ', error)
        return [];
    }
}

const TeamSelector = () => {

    const [team, setTeam] = useState<string[]>([]);
    
    useEffect(() => {
        getTeamData().then((result) => {
            setTeam(result);
         }).catch((error) => {
            console.log("Error fetching data", error)
         });
    }, []);

    
    return (
        <>
            <p>This is the team selector page. The best team is: </p>
            {team.map((player) => <p> -  {player} </p> )}
        </>
    )
};

export default TeamSelector