import {Link} from "react-router-dom";

const Home = () => {
    return (
        <>
            <p>This is the home page.</p>
            <p><Link to="/team-selector/">Go to team selector</Link></p>
        </>
    )
};

export default Home