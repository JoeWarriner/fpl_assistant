import renderer from 'react-test-renderer';
import React from 'react';
import {Player} from '../types/player';
import { PlayerTable } from '../components/tables';
import * as Backend from '../services/backend';
import { render } from 'react-dom';

jest.mock('../services/backend')


const getPlayersMock = async (pageSize: number, offSet: number): Promise<Player[]> => {
    const players: Player[] = await new Promise((resolve, reject) => {
            process.nextTick(() => {
                let testPlayer: Player
                if (offSet == 1){
                    testPlayer = {
                        id: 1,
                        first_name: 'Billy',
                        second_name: 'Bob',
                        position: 'DEF',
                        team: 'ARS',
                        predicted_score: 5,
                        current_value: 5
    
                    }
                } else {
                    testPlayer = {
                        id: 2,
                        first_name: 'Sammy',
                        second_name: 'Ray',
                        position: 'FWD',
                        team: 'ARS',
                        predicted_score: 5,
                        current_value: 5
                    }
                }
                testPlayer ?
                resolve([testPlayer]) :
                reject({error: 'Something went wrong'})
            })
            }
        )
        return players
    

}



it('player table offset 1', async () => {
    let table: renderer.ReactTestRenderer | undefined;
    

    await renderer.act(() => {
            table = renderer.create(<PlayerTable 
                                        offSet={1}
                                        pageSize={1}
                                        dataGetter={getPlayersMock} 
                                    />)
    })
   
    
    let tree = table?.toJSON()
    expect(tree).toMatchSnapshot();

    
    await renderer.act(() => {
        table = renderer.create(<PlayerTable 
                    offSet={2}
                    pageSize={1}
                    dataGetter={getPlayersMock} 
                />)
            })

    tree = table?.toJSON()
    expect(tree).toMatchSnapshot();



    })

