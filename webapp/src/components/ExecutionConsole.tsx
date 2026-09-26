import {
  Box,
  Card,
  CardContent,
  LinearProgress,
  Stack,
  Typography
} from "@mui/material";

type ExecutionConsoleProps = {
  title?: string;
  progress: number;
  logs: string[];
  running: boolean;
  currentPhase?: string;
};

export default function ExecutionConsole({
  title = "Verbose Execution Log",
  progress,
  logs,
  running,
  currentPhase = "idle"
}: ExecutionConsoleProps) {
  return (
    <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
      <CardContent>
        <Stack spacing={1.2}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            {title}
          </Typography>

          <LinearProgress variant="determinate" value={progress} />
          <Typography variant="caption" color="text.secondary">
            Progress: {progress}% {running ? "(running)" : "(idle)"}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            Current Phase: {currentPhase}
          </Typography>

          <Box
            className="mono"
            sx={{
              backgroundColor: "#0f172a",
              color: "#d6e8ff",
              borderRadius: 1,
              p: 1.5,
              height: 200,
              overflow: "auto",
              fontSize: 12
            }}
          >
            {logs.length > 0 ? logs.join("\n") : "No logs yet."}
          </Box>
        </Stack>
      </CardContent>
    </Card>
  );
}
