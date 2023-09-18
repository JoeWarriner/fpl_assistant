import {Outlet, Link, useOutlet} from "react-router-dom";
import { Dropdown, Navbar, Nav, NavDropdown, Container, Row, Col} from "react-bootstrap";
// import  Container from "react-bootstrap/Container";
// import Row from "react-bootstrap/Row";
// import Col from "react-bootstrap/Col";

const Home = () => {
    const outlet = useOutlet();
    return (
        <>
            <Navbar expand="lg" bg="dark" data-bs-theme="dark">
                <Container>
                    <Navbar.Brand href="/" className="text-light">Fantasy Football Assistant</Navbar.Brand>
                        <Navbar.Toggle aria-controls="navbar-nav" />
                        <Navbar.Collapse id="navbar-nav">
                            <Nav className="ms-auto">
                                <Nav.Link href="/">Home</Nav.Link>
                                <Nav.Link href="/team-selector/">Team</Nav.Link>
                                <Nav.Link href="/player_data">Player Data</Nav.Link>
                            </Nav>
                            </Navbar.Collapse>
                </Container>
            </Navbar>
            <Container>
                <Row className="p-3">
                    <Col>
                    {outlet || 'Welcome the the fantasy league assistant!'}
                    </Col>
                </Row>
            </Container>
    
        </>   
    );
}

export default Home