# New York City Subway Chatbot for Claude Desktop

This demo uses the MTA's freely available [data feeds](https://new.mta.info/developers) for the New York City Subway to build a passenger chatbot. The static GTFS subway schedule is loaded into a [Turbopuffer](https://turbopuffer.com/) namespace for vector and full-text search.

The chatbot is powered by Claude Desktop configured with a simple Turbopuffer Model Context Protocol (MCP) server, which enables Claude to search through the subway timetable data stored in Turbopuffer. This integration allows Claude to retrieve highly relevant schedule and route information to provide accurate, contextual responses to passenger queries about the NYC subway timetable. The chatbot also happens to be a native New Yawker. Capisce! 

## 🚇 Overview

This project combines NYC MTA's real-time subway data with AI-powered chat functionality, allowing users to ask natural language questions about subway schedules, routes, and station information. The system uses:

- **GTFS Data**: Real-time subway schedules and route information from the MTA
- **Full-Text Search**: Turbopuffer for fast semantic search across transit data
- **Claude Integration**: RAG-powered chat through Model Context Protocol (MCP)

## 📦 Installation

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- [Turbopuffer](https://turbopuffer.com/) account and API key
- Claude Desktop (for chat interface)

### Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/jrkinley/mta-rag-chat.git
   cd mta-rag-chat
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Download MTA GTFS data**:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

4. **Configure environment variables**:
   Create a `.env` file with your Turbopuffer credentials:
   ```bash
   TURBOPUFFER_API_KEY=your_api_key_here
   TURBOPUFFER_BASE_URL=your_region.turbopuffer.com
   ```

5. **Load the MTA GTFS data into Turbopuffer**:

   The `load_gtfs_data.ipynb` Jupyter notebook provides a step-by-step guide for loading MTA GTFS data into Turbopuffer:

7. **Configure Claude Desktop**: 

   Add the following to `~/Library/Application Support/Claude/claude_desktop_config.json`:

     ```json
     {
       "mcpServers": {
           "turbopuffer": {
             "command": "uv",
             "args": [
                 "--directory",
                 "/absolute/path/to/mta-rag-chat/turbopuffer-mcp/",
                 "run",
                 "turbopuffer_mcp.py"
             ],
             "env": {
                 "TURBOPUFFER_API_KEY": "your_token_here",
                 "TURBOPUFFER_BASE_URL": "your_region.turbopuffer.com"
             }
           }
       }
     }
     ```

7. **Restart Claude Desktop** to load the MCP server

## 🤖 Start chatting to Claude

### Claude's responses with tools disabled

<div style="display: flex; justify-content: space-between;">
  <img src="images/claude_1a.png" width="32%" alt="Claude's response with tools disabled">
  <img src="images/claude_1b.png" width="32%" alt="Claude's response with tools disabled">
  <img src="images/claude_1c.png" width="32%" alt="Claude's response with tools disabled">
</div>
</br>

Even when tools are disabled, Claude provides good advice about subway routes to point passengers in the right direction, but it falls short when asked about real-time information or specific schedule details.

### Claude's responses with tools enabled

<div style="display: flex; justify-content: space-between; align-items: flex-start;">
  <img src="images/claude_2a.png" width="32%" alt="Claude's response with tools enabled">
  <img src="images/claude_2b.png" width="32%" alt="Claude's response with tools enabled">
  <img src="images/claude_2c.png" width="32%" alt="Claude's response with tools enabled">
</div>
</br>

When tools are enabled, Claude can access the GTFS schedule data stored in Turbopuffer through the MCP server. This allows Claude to provide much more detailed and accurate responses with specific schedule times, route numbers, and station names. Claude can now confidently answer questions about:

- Exact departure and arrival times for specific routes
- Transfer points and connection details between lines
- Station-specific schedule information
- Service patterns for different times of day
- Real-time schedule data from the MTA feed

The combination of Claude's natural language understanding and direct access to transit data through Turbopuffer creates a much more capable and reliable subway information assistant. Rather than providing general guidance, Claude can now give precise, actionable information to help passengers plan their subway journeys effectively.


## 🚀 Future Enhancements

To make the subway chatbot even more helpful for passengers, here are some potential enhancements that could be implemented:

### Real-time Service Updates
- Integrate the MTA's GTFS-RT real-time feeds (https://api.mta.info/#/subwayRealTimeFeeds) to provide:
  - Live train arrival predictions and delays
  - Service disruptions and engineering works
  - Temporary route changes and diversions
  - Platform changes and station closures
- Enable Claude to proactively warn about disruptions affecting suggested routes

### Emergency Services Integration
- Connect to NYC emergency services data feeds to:
  - Alert passengers about stations to avoid due to police activity
  - Provide alternate routes during emergency situations
  - Warn about station entrances/exits affected by incidents
  - Share safety advisories from NYPD and FDNY

### Social Context
- Incorporate real-time social media feeds to add passenger context:
  - Twitter/X mentions of specific stations or lines
  - Crowdsourced reports of platform conditions
  - Station amenity status (elevators, escalators)
  - Special events causing increased crowding
- Allow Claude to factor social insights into route recommendations

### Enhanced Visualization
- Generate custom route maps for complex journeys
- Provide platform-specific walking directions within stations
- Show alternative route options side-by-side
- Highlight areas of construction or disruption
