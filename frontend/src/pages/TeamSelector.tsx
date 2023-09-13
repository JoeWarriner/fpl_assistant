import { getOptimalTeam } from '../services/backend';
import { PlayerTable } from '../components/tables';




const TeamSelector = () => {

    
    return (
        <>
            <h1> Team Selector </h1>
            < PlayerTable
                dataGetter={getOptimalTeam}
                pageSize={15}
                offSet={0}
                />
        </>
    );
}

export default TeamSelector;