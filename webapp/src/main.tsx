import React from "react";
import ReactDOM from "react-dom/client";
import { CssBaseline, ThemeProvider, createTheme } from "@mui/material";
import App from "./App";
import "./styles.css";

const theme = createTheme({
  typography: {
    fontFamily: '"Space Grotesk", "Segoe UI", sans-serif'
  },
  palette: {
    mode: "light",
    primary: {
      main: "#0f766e"
    },
    secondary: {
      main: "#c2410c"
    },
    background: {
      default: "#f4f7f8",
      paper: "#ffffff"
    }
  },
  shape: {
    borderRadius: 14
  }
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <App />
    </ThemeProvider>
  </React.StrictMode>
);
