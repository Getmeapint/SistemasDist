#!/usr/bin/env node
// Simple script to open a WebSocket and log messages quietly.
// Usage: node ws-listen.js ws://host:port/ws?race=race-trail_route_1
import WebSocket from 'ws';

const url = process.argv[2];
if (!url) {
  console.error('Usage: node ws-listen.js ws://host:port/ws?race=...'); 
  process.exit(1);
}

function connect() {
  const ws = new WebSocket(url);

  ws.on('open', () => {
    console.log(`Connected to ${url}`);
  });

  ws.on('message', (data) => {
    // Comment out to reduce noise
    // console.log(data.toString());
  });

  ws.on('close', () => {
    console.log('Connection closed, retrying in 5s');
    setTimeout(connect, 5000);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message || err, 'retrying in 5s');
    ws.close();
    setTimeout(connect, 5000);
  });
}

connect();
