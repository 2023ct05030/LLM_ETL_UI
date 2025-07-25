import React from 'react';
import ReactDOM from 'react-dom/client';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import App from './App';

const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: {
      main: '#00bcd4', // Cyan
      light: '#4dd0e1',
      dark: '#00838f',
    },
    secondary: {
      main: '#00e5ff', // Light cyan accent
      light: '#40c4ff',
      dark: '#0091ea',
    },
    background: {
      default: '#0a0a0a', // Very dark background
      paper: '#1a1a1a', // Dark paper background
    },
    text: {
      primary: '#ffffff',
      secondary: '#b0bec5',
    },
    divider: '#333333',
    action: {
      hover: 'rgba(0, 188, 212, 0.08)',
      selected: 'rgba(0, 188, 212, 0.16)',
    },
  },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: 8,
        },
        contained: {
          boxShadow: '0 4px 12px rgba(0, 188, 212, 0.3)',
          '&:hover': {
            boxShadow: '0 6px 16px rgba(0, 188, 212, 0.4)',
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
          borderRadius: 12,
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            '& fieldset': {
              borderColor: '#333333',
            },
            '&:hover fieldset': {
              borderColor: '#00bcd4',
            },
            '&.Mui-focused fieldset': {
              borderColor: '#00bcd4',
            },
          },
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          backgroundColor: 'rgba(0, 188, 212, 0.15)',
          color: '#00e5ff',
          border: '1px solid rgba(0, 188, 212, 0.3)',
        },
      },
    },
  },
});

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

root.render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <App />
    </ThemeProvider>
  </React.StrictMode>
);
