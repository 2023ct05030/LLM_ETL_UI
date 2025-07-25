#!/bin/bash

# Start the LLM ETL Frontend Application

echo "🚀 Starting LLM ETL Frontend Application..."

# Check if Python virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Install Python dependencies
# echo "📦 Installing Python dependencies..."
# uv add -r requirements.txt

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your actual credentials before running the application!"
fi

# Start backend server
echo "🖥️  Starting backend server..."
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!

# Wait for backend to start
echo "⏳ Waiting for backend to start..."
sleep 5

# Install frontend dependencies if not already installed
if [ ! -d "frontend/node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    cd frontend
    npm install
    cd ..
fi

# Start frontend
echo "🎨 Starting frontend..."
cd frontend
npm start &
FRONTEND_PID=$!

echo "✅ Application started successfully!"
echo "🔗 Backend API: http://localhost:8000"
echo "🔗 Frontend: http://localhost:3000"
echo "📚 API Documentation: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop the application"

# Function to cleanup processes
cleanup() {
    echo "🛑 Stopping application..."
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    exit 0
}

# Set trap to cleanup on exit
trap cleanup INT TERM

# Wait for processes
wait
