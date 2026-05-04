# DC_RANCE: Intelligent Routing and Traffic Management System

## Overview
DC_RANCE is a backend-driven system designed to simulate and manage intelligent routing and traffic behavior using algorithmic decision-making. The project demonstrates how structured logic and real-time computation can optimize movement, reduce congestion, and improve system efficiency.

The system integrates a Python-based backend with a lightweight dashboard interface to provide visualization and interaction with the routing logic.

## Features
- Algorithm-based routing and decision-making
- Modular backend architecture for scalability
- Interactive dashboard for visualization
- Separation of logic and presentation layers
- Extensible design for integrating advanced algorithms

## Project Structure

DC_RANCE/
├── backend/
│   ├── server.py
│   └── engine/
│       ├── __init__.py
│       └── ...
├── frontend/
│   └── dashboard.html
├── requirements.txt
├── README.md
└── .gitignore

## Technology Stack
- Backend: Python
- Frontend: HTML
- Libraries: Defined in requirements.txt

## Installation and Setup

### 1. Clone the Repository
git clone https://github.com/your-username/DC_RANCE.git
cd DC_RANCE

### 2. Create a Virtual Environment
python -m venv venv

Activate the environment:

On Windows:
venv\Scripts\activate

On macOS/Linux:
source venv/bin/activate

### 3. Install Dependencies
pip install -r requirements.txt

## Running the Application
python server.py

Once the server starts, open a web browser and navigate to:
http://127.0.0.1:5000

If the dashboard does not load automatically, open the dashboard.html file manually in a browser.

## System Workflow
1. The backend server initializes and listens for incoming requests.
2. The engine module processes input using defined routing logic.
3. The system applies decision-making algorithms to compute optimized results.
4. The dashboard displays the processed output for user interaction.

## Configuration
If the default port is already in use, update the port number in server.py:
app.run(port=5001)

## Common Issues and Troubleshooting

### Missing Dependencies
pip install -r requirements.txt

### Server Not Starting
- Verify Python installation
- Check for syntax errors in server.py

### Dashboard Not Loading
- Confirm the server is running
- Open dashboard.html manually if required

## Future Enhancements
- Integration with real-time data sources
- Implementation of advanced routing algorithms such as A* and Dijkstra
- Improved user interface and visualization features
- Deployment as a scalable web application

## Author
Kaniska C
GitHub: https://github.com/kaniska5
