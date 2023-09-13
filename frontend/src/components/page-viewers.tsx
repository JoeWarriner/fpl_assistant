import type {Player} from '../types/player';
import {DataGetter} from  '../services/backend';
import {PlayerTable} from './tables'
import Button from 'react-bootstrap/Button';
import React, { useState, useEffect } from 'react';



interface PageViewerProps {
    dataGetter: DataGetter,
    pageSize: number
}

export function PlayerPageViewer(props: PageViewerProps){

    const [offSet, setOffSet] = useState<number>(0);

    const onClickPrevious = () => {
        setOffSet(offSet - 10);
    }
    const onClickNext = () => {
        setOffSet(offSet + 10);
    }

    return (
        <div>
            <PlayerTable 
                dataGetter={props.dataGetter}
                pageSize={props.pageSize}
                offSet={offSet} 
            />
            <Button
                onClick={onClickPrevious}
            >Previous</Button>
            <Button 
                onClick={onClickNext}
            >Next</Button>
        </div>
    )

        

}
