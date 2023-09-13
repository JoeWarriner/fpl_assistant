import { PlayerPageViewer } from '../components/page-viewers';
import { getPlayers } from '../services/backend';

const PlayerData = () => {

    return (
        <div>
            <h1>Player Data </h1>
            <PlayerPageViewer 
                pageSize={10}
                dataGetter={getPlayers}/>
        </div>
    )
}

export default PlayerData;
