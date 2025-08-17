import React, { useEffect, useRef, useState } from "react";
import {
  AppBar, Toolbar, Typography, Box, Paper, List, ListItemButton, ListItemIcon,
  ListItemText, IconButton, Chip, Alert, Button, Divider, Card, CardContent,
  Tooltip, CircularProgress, TextField, Table, TableHead, TableBody, TableRow, TableCell,
} from "@mui/material";
import {
  CloudUpload, Chat as ChatIcon, Code as CodeIcon, Route,
  Brightness2, Settings, FileCopy, Download, Storage, Clear, PlayArrow, Send, Refresh,
} from "@mui/icons-material";
import { useDropzone } from "react-dropzone";
import axios from "axios";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";



axios.defaults.baseURL = process.env.REACT_APP_API_URL || "http://localhost:8000";

type Tab = "upload" | "chat" | "code" | "workflow";

interface Message {
  id: string;
  type: "user" | "assistant";
  content: string;
  timestamp: Date;
}

interface FileInfo {
  s3_url: string;
  filename: string;
  original_filename: string;
  content_type: string;
  size: number;
}

interface ProfilingColumn {
  name: string;
  dtype?: string;
  nulls?: number;
  distinct?: number;
  min?: any;
  max?: any;
  mean?: number;
}

interface ProfilingPayload {
  columns?: ProfilingColumn[];
  primary_key_candidates?: string[];
  date_columns?: string[];
  total_rows?: number;
  data_quality?: string;
}

interface DagStep {
  step: string;
  status?: string;
  note?: string;
}

const formatMessageContent = (content: string) => (
  <>
    {content.split("\n").map((line, i) => {
      if (!line.trim()) return <br key={`br-${i}`} />;
      if (line.endsWith(":") || (line === line.toUpperCase() && line.length > 3)) {
        return (
          <Typography key={i} variant="subtitle2" sx={{ fontWeight: 600, color: "primary.light", mt: i ? 2 : 0, mb: 1 }}>
            {line}
          </Typography>
        );
      }
      if (/^\s*[-*•]\s/.test(line) || /^\s*\d+\.\s/.test(line)) {
        return (
          <Typography
            key={i}
            variant="body2"
            sx={{
              ml: 2,
              mb: 0.5,
              "&:before": { content: '"•"', color: "primary.main", fontWeight: "bold", mr: "8px" },
            }}
          >
            {line.replace(/^\s*[-*•]\s/, "").replace(/^\s*\d+\.\s/, "")}
          </Typography>
        );
      }
      if (/(s3:\/\/|\.py|ERROR:|INFO:)/.test(line)) {
        return (
          <Typography
            key={i}
            variant="body2"
            sx={{
              fontFamily: "monospace",
              bgcolor: "rgba(0, 188, 212, 0.1)",
              px: 1,
              py: 0.5,
              borderRadius: 1,
              fontSize: "0.85rem",
              my: 0.5,
              wordBreak: "break-all",
            }}
          >
            {line}
          </Typography>
        );
      }
      return (
        <Typography key={i} variant="body1" sx={{ mb: 0.5, lineHeight: 1.6 }}>
          {line}
        </Typography>
      );
    })}
  </>
);

const App: React.FC = () => {
  const [tab, setTab] = useState<Tab>("upload");

  const [messages, setMessages] = useState<Message[]>([
    {
      id: "1",
      type: "assistant",
      content:
        "Hello! Upload a file (CSV/JSON/Excel/Parquet) and tell me what ETL you want. I’ll push to S3 and generate Snowflake ingestion code.",
      timestamp: new Date(),
    },
  ]);

  // === NEW: dedicated states so Code/Profiling/Workflow are independent of chat ===
  const [latestCode, setLatestCode] = useState<string>("");           // only ETL code
  const [profiling, setProfiling] = useState<ProfilingPayload | null>(null);
  const [workflow, setWorkflow] = useState<any | null>(null);         // full workflow response
  const [dag, setDag] = useState<DagStep[] | null>(null);

  const [inputMessage, setInputMessage] = useState("");
  const [currentFile, setCurrentFile] = useState<FileInfo | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [isSending, setIsSending] = useState(false);
  const [isRunningWorkflow, setIsRunningWorkflow] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const endRef = useRef<HTMLDivElement>(null);

  const isChatStarted = messages.length > 1;

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isSending]);

  // -------- Upload ----------
  const onDrop = async (acceptedFiles: File[]) => {
    if (!acceptedFiles.length) return;
    const file = acceptedFiles[0];

    setIsUploading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const { data } = await axios.post<FileInfo>("/upload", formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });

      setCurrentFile(data);
      setMessages((prev) => [
        ...prev,
        { id: crypto.randomUUID(), type: "user", content: `Uploaded file: ${file.name}`, timestamp: new Date() },
        {
          id: crypto.randomUUID(),
          type: "assistant",
          content: `File successfully uploaded to S3: ${data.s3_url}\n\nFile details:\n- Size: ${(
            data.size / 1024
          ).toFixed(2)} KB\n- Type: ${data.content_type}\n\nNow describe the ETL operations you want.`,
          timestamp: new Date(),
        },
      ]);
    } catch (err: any) {
      setError(
        err?.response
          ? `Upload failed: ${err.response.data?.detail || err.response.statusText}`
          : err?.request
          ? "Upload failed: cannot connect to server (port 8000?)."
          : `Upload failed: ${err?.message}`
      );
    } finally {
      setIsUploading(false);
    }
  };

  const { getRootProps, getInputProps, isDragActive, open: openPicker } = useDropzone({
    onDrop,
    noClick: true,
    accept: {
      "text/csv": [".csv"],
      "application/json": [".json"],
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"],
      "application/vnd.ms-excel": [".xls"],
      "text/plain": [".txt"],
      "application/parquet": [".parquet"],
    },
    multiple: false,
  });

  // -------- Chat ----------
  const sendMessage = async () => {
    if (!inputMessage.trim()) return;

    const userMsg: Message = {
      id: crypto.randomUUID(),
      type: "user",
      content: inputMessage,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMsg]);
    setInputMessage("");
    setIsSending(true);
    setError(null);

    try {
      const { data } = await axios.post("/chat", {
        message: userMsg.content,
        file_url: currentFile?.s3_url,
        file_name: currentFile?.original_filename,
      });

      // keep Chat output in messages
      setMessages((prev) => [
        ...prev,
        {
          id: crypto.randomUUID(),
          type: "assistant",
          content: data.response,
          timestamp: new Date(),
        },
      ]);

      // keep Code output isolated
      if (data.etl_code) {
        setLatestCode(data.etl_code);
        
        // Add message directing user to Code tab
        setMessages((prev) => [
          ...prev,
          {
            id: crypto.randomUUID(),
            type: "assistant",
            content: "💻 ETL code has been generated! Check the 'Code' tab to view, copy, or download the complete Python script.",
            timestamp: new Date(),
          },
        ]);
      }
    } catch (err: any) {
      setError(
        err?.response
          ? `Chat failed: ${err.response.data?.detail || err.response.statusText}`
          : err?.request
          ? "Chat failed: cannot connect to server (port 8000?)."
          : `Chat failed: ${err?.message}`
      );
    } finally {
      setIsSending(false);
    }
  };

  // -------- Workflow ----------
  const runWorkflow = async () => {
    if (!currentFile || !inputMessage.trim()) {
      setError("Please upload a file and provide requirements before running the workflow.");
      return;
    }
    setIsRunningWorkflow(true);
    setError(null);

    try {
      const body = {
        file_url: currentFile.s3_url,
        file_name: currentFile.original_filename,
        requirements: inputMessage,
        auto_execute: true,
      };
      const { data } = await axios.post("/etl-workflow", body);

      // Persist workflow + profiling for their tabs
      setWorkflow(data);
      setProfiling(data.profiling ?? null);
      setDag(data.dag ?? null);

      // Set ETL code if available in the workflow response
      if (data.etl_code) {
        setLatestCode(data.etl_code);
      } else if (data.script_content) {
        setLatestCode(data.script_content);
      } else if (data.generated_code) {
        setLatestCode(data.generated_code);
      }

      // Also post a summary message back in Chat
      const parts: string[] = [
        "🚀 ETL Workflow Completed",
        `Workflow ID: ${data.workflow_id ?? "N/A"}`,
        `Success: ${data.success ? "✅ Yes" : "❌ No"}`,
        data.timestamp ? `Timestamp: ${new Date(data.timestamp).toLocaleString()}` : "",
        data.script_path ? `Script Generated: ${data.script_path}` : "",
        data.execution_success !== undefined ? `Execution: ${data.execution_success ? "✅ Success" : "❌ Failed"}` : "",
        data.snowflake_success !== undefined ? `Snowflake: ${data.snowflake_success ? "✅ Success" : "❌ Failed"}` : "",
        data.records_inserted ? `Records Inserted: ${data.records_inserted}` : "",
      ].filter(Boolean);

      if (data.summary) parts.push(`\nSummary:\n${data.summary}`);
      
      // Add navigation instructions
      parts.push("\n📋 Navigation Guide:");
      parts.push("• View this summary and execution details in the 'Workflow' tab");
      parts.push("• Find the generated ETL code in the 'Code' tab");
        if (data.profiling && (data.profiling.columns?.length || data.profiling.primary_key_candidates?.length || data.profiling.date_columns?.length)) {
          parts.push("• Check data profiling, schema analysis, and column statistics in the 'Profiling' tab");
        }
      
      setMessages((prev) => [
        ...prev,
        { id: crypto.randomUUID(), type: "assistant", content: parts.join("\n"), timestamp: new Date() },
      ]);

      // Jump user to Workflow tab to view structured results
      setTab("workflow");
      setInputMessage("");
    } catch (err: any) {
      setError(
        err?.response?.data?.message
          ? `Workflow failed: ${err.response.data.message}`
          : err?.response?.data?.detail
          ? `Workflow failed: ${err.response.data.detail}`
          : err?.request
          ? "Workflow failed: cannot connect to server (port 8000?)."
          : `Workflow failed: ${err?.message}`
      );
    } finally {
      setIsRunningWorkflow(false);
    }
  };

  const copyToClipboard = (text: string) => navigator.clipboard.writeText(text);
  const downloadCode = (code: string, filename = "etl_code.py") => {
    const blob = new Blob([code], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  const loadLatestScript = async () => {
    try {
      setError(null);
      const { data } = await axios.get("/latest-script");
      if (data.script_content) {
        setLatestCode(data.script_content);
        setMessages((prev) => [
          ...prev,
          {
            id: crypto.randomUUID(),
            type: "assistant",
            content: `📄 Latest generated script loaded: ${data.filename || 'etl_script.py'}\nScript is now available in the Code tab.`,
            timestamp: new Date(),
          },
        ]);
      } else {
        setError("No script content found in the latest generated file.");
      }
    } catch (err: any) {
      setError(
        err?.response?.data?.detail
          ? `Failed to load script: ${err.response.data.detail}`
          : err?.request
          ? "Failed to load script: cannot connect to server."
          : `Failed to load script: ${err?.message}`
      );
    }
  };
  const clearChat = () => {
    setMessages([
      {
        id: "1",
        type: "assistant",
        content:
          "Hello! Upload a file (CSV/JSON/Excel/Parquet) and tell me what ETL you want. I’ll push to S3 and generate Snowflake ingestion code.",
        timestamp: new Date(),
      },
    ]);
    setLatestCode("");
    setProfiling(null);
    setWorkflow(null);
    setDag(null);
    setCurrentFile(null);
    setError(null);
    setTab("upload");
  };

  const sidebarWidth = 240;

  return (
    <Box sx={{ bgcolor: "#0B1220", minHeight: "100vh", color: "rgba(255,255,255,0.85)" }}>
      {/* Top bar */}
      <AppBar elevation={0} position="sticky" sx={{ bgcolor: "rgba(11,18,32,0.8)", backdropFilter: "blur(6px)", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
        <Toolbar sx={{ maxWidth: 1280, mx: "auto", width: "100%" }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1.25 }}>
            <Box sx={{ width: 28, height: 28, borderRadius: 2, bgcolor: "rgba(56,189,248,0.15)", border: "1px solid rgba(125,211,252,0.35)", display: "grid", placeItems: "center" }}>
              <CloudUpload sx={{ fontSize: 16, color: "rgb(125,211,252)" }} />
            </Box>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              LLM ETL Studio
            </Typography>
            <Typography variant="body2" sx={{ color: "rgba(255,255,255,0.55)" }}>
              — Dark UI (backend)
            </Typography>
          </Box>
          <Box sx={{ ml: "auto", display: "flex", gap: 1 }}>
            <IconButton size="small" sx={{ color: "rgba(255,255,255,0.8)" }}>
              <Brightness2 fontSize="small" />
            </IconButton>
            <IconButton size="small" sx={{ color: "rgba(255,255,255,0.8)" }}>
              <Settings fontSize="small" />
            </IconButton>
            <Tooltip title="Clear chat">
              <IconButton size="small" onClick={clearChat} sx={{ color: "rgba(255,255,255,0.8)" }}>
                <Clear fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
        </Toolbar>
      </AppBar>

      <Box sx={{ maxWidth: 1280, mx: "auto", display: "grid", gridTemplateColumns: { md: `${sidebarWidth}px 1fr` }, gap: 2, px: 2, py: 3 }}>
        {/* Sidebar */}
        <Paper
          sx={{
            display: { xs: "none", md: "block" },
            height: "fit-content",
            alignSelf: "start",
            bgcolor: "rgba(2,6,23,0.6)",
            border: "1px solid rgba(255,255,255,0.08)",
            width: sidebarWidth,
            borderRadius: 3,
            overflow: "hidden",
          }}
          elevation={0}
        >
          <List sx={{ p: 1 }}>
            <ListItemButton selected={tab === "upload"} onClick={() => setTab("upload")} sx={{ borderRadius: 2 }}>
              <ListItemIcon><CloudUpload sx={{ color: tab === "upload" ? "cyan.300" : "rgba(255,255,255,0.6)" }} /></ListItemIcon>
              <ListItemText primary="Upload" />
            </ListItemButton>
            <ListItemButton selected={tab === "chat"} onClick={() => setTab("chat")} sx={{ borderRadius: 2 }}>
              <ListItemIcon><ChatIcon sx={{ color: tab === "chat" ? "cyan.300" : "rgba(255,255,255,0.6)" }} /></ListItemIcon>
              <ListItemText primary="Chat" />
            </ListItemButton>
            <ListItemButton selected={tab === "code"} onClick={() => setTab("code")} sx={{ borderRadius: 2 }}>
              <ListItemIcon><CodeIcon sx={{ color: tab === "code" ? "cyan.300" : "rgba(255,255,255,0.6)" }} /></ListItemIcon>
              <ListItemText primary="Code" />
            </ListItemButton>
            <ListItemButton selected={tab === "workflow"} onClick={() => setTab("workflow")} sx={{ borderRadius: 2 }}>
              <ListItemIcon><Route sx={{ color: tab === "workflow" ? "cyan.300" : "rgba(255,255,255,0.6)" }} /></ListItemIcon>
              <ListItemText primary="Workflow" />
            </ListItemButton>
          </List>
        </Paper>

        {/* Main */}
        <Box>
          {/* Upload pane */}
          {tab === "upload" && (
            <Paper elevation={0} sx={{ p: { xs: 3, md: 4 }, bgcolor: "rgba(2,6,23,0.6)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 3 }}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, mb: 1 }}>
                <Box sx={{ width: 36, height: 36, borderRadius: 2, bgcolor: "rgba(56,189,248,0.15)", border: "1px solid rgba(125,211,252,0.35)", display: "grid", placeItems: "center" }}>
                  <CloudUpload sx={{ fontSize: 20, color: "rgb(125,211,252)" }} />
                </Box>
                <Typography variant="h4" sx={{ fontWeight: 700 }}>Upload & Auto-Profile</Typography>
              </Box>
              <Typography sx={{ color: "rgba(255,255,255,0.65)", mb: 3 }}>
                Drop any CSV/Parquet/Excel. This sends to your backend and returns metadata.
              </Typography>

              <Box
                {...getRootProps()}
                sx={{
                  border: "2px dashed rgba(148,163,184,0.35)", borderRadius: 3, bgcolor: "rgba(15,23,42,0.5)",
                  py: 8, px: 2, textAlign: "center", transition: "150ms ease", "&:hover": { borderColor: "rgba(203,213,225,0.6)" }, mb: 2,
                }}
              >
                <input {...getInputProps()} />
                <CloudUpload sx={{ color: "rgb(125,211,252)", fontSize: 32, mb: 1 }} />
                <Typography sx={{ mb: 1, fontSize: 16 }}>{isDragActive ? "Drop to upload…" : "Drag & drop here or"}</Typography>
                <Button onClick={openPicker} variant="contained" sx={{ bgcolor: "rgb(2,132,199)" }} disabled={isUploading}>
                  {isUploading ? "Uploading..." : "Choose File"}
                </Button>
              </Box>

              <Paper variant="outlined" sx={{ p: 2, borderRadius: 2, borderColor: "rgba(255,255,255,0.08)", bgcolor: "rgba(2,6,23,0.5)" }}>
                <Typography variant="body2" sx={{ color: "rgba(255,255,255,0.7)" }}>
                  {currentFile ? (
                    <>
                      <strong>{currentFile.original_filename}</strong> • {currentFile.content_type} • {(currentFile.size / 1024).toFixed(2)} KB
                      <br />
                      <span style={{ opacity: 0.8, wordBreak: "break-all" }}>{currentFile.s3_url}</span>
                    </>
                  ) : (
                    "Connected to backend • Ready for uploads"
                  )}
                </Typography>
              </Paper>
            </Paper>
          )}

          {/* File status + errors */}
          {currentFile && (
            <Alert severity="success" icon={<Storage />} sx={{ mt: 2, bgcolor: "rgba(0,188,212,0.10)", border: "1px solid rgba(0,188,212,0.25)", color: "rgba(255,255,255,0.9)", "& .MuiAlert-icon": { color: "primary.main" } }}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, flexWrap: "wrap" }}>
                <Typography sx={{ fontWeight: 500 }}>
                  <strong>File uploaded:</strong> {currentFile.original_filename}
                </Typography>
                <Chip label={currentFile.content_type} size="small" />
                <Chip label={`${(currentFile.size / 1024).toFixed(2)} KB`} size="small" />
                <Chip label="Ready for ETL" size="small" color="success" variant="outlined" />
              </Box>
            </Alert>
          )}
          {error && (
            <Alert sx={{ mt: 2 }} severity="error" onClose={() => setError(null)}>
              {error}
            </Alert>
          )}

          {/* CHAT pane (conversation only) */}
          {tab === "chat" && (
            <Paper elevation={0} sx={{ mt: 2, p: 3, bgcolor: "rgba(2,6,23,0.6)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 3 }}>
              <Box
                sx={{
                  maxHeight: 420,
                  overflow: "auto",
                  pr: 1,
                  "&::-webkit-scrollbar": { width: 6 },
                  "&::-webkit-scrollbar-thumb": { bgcolor: "rgba(148,163,184,0.4)", borderRadius: 3 },
                }}
              >
                {messages.map((m) => (
                  <Box key={m.id} sx={{ mb: 2 }}>
                    <Card
                      elevation={0}
                      sx={{
                        maxWidth: { xs: "95%", sm: "85%" },
                        ml: m.type === "user" ? "auto" : 0,
                        mr: m.type === "assistant" ? "auto" : 0,
                        bgcolor: m.type === "user" ? "rgba(2,132,199,0.12)" : "rgba(255,255,255,0.06)",
                        border: m.type === "user" ? "1px solid rgba(56,189,248,0.35)" : "1px solid rgba(255,255,255,0.08)",
                        borderRadius: 2,
                        wordWrap: "break-word",
                        overflowWrap: "break-word",
                      }}
                    >
                      <CardContent sx={{ p: 2.5 }}>
                        <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", mb: 1, flexWrap: "wrap", gap: 1 }}>
                          <Chip size="small" label={m.type === "user" ? "You" : "Assistant"} variant="outlined" />
                          <Typography variant="caption" sx={{ opacity: 0.7, fontSize: "0.75rem" }}>
                            {m.timestamp.toLocaleTimeString()}
                          </Typography>
                        </Box>
                        <Box sx={{ wordWrap: "break-word", overflowWrap: "break-word" }}>
                          {formatMessageContent(m.content)}
                        </Box>
                      </CardContent>
                    </Card>
                  </Box>
                ))}
                {isSending && (
                  <Box sx={{ mb: 1, opacity: 0.8 }}>
                    <Card elevation={0} sx={{ maxWidth: { xs: "95%", sm: "85%" }, bgcolor: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.08)" }}>
                      <CardContent sx={{ py: 2, px: 2.5, display: "flex", alignItems: "center", gap: 1.5 }}>
                        <CircularProgress size={18} />
                        <Typography>Assistant is generating response…</Typography>
                      </CardContent>
                    </Card>
                  </Box>
                )}
                <div ref={endRef} />
              </Box>

              <Box sx={{ mt: 2, display: "flex", flexDirection: { xs: "column", sm: "row" }, gap: 1.5 }}>
                <TextField
                  fullWidth
                  multiline
                  maxRows={4}
                  minRows={1}
                  value={inputMessage}
                  placeholder="Describe your ETL requirements…"
                  onChange={(e) => setInputMessage(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      sendMessage();
                    }
                  }}
                  sx={{
                    "& .MuiOutlinedInput-root": {
                      bgcolor: "rgba(255,255,255,0.05)",
                      "&:hover": { bgcolor: "rgba(255,255,255,0.08)" },
                      "&.Mui-focused": { bgcolor: "rgba(255,255,255,0.08)" },
                    },
                  }}
                />
                <Box sx={{ display: "flex", flexDirection: { xs: "row", sm: "row" }, gap: 1, minWidth: { sm: "auto" } }}>
                  <Button 
                    variant="contained" 
                    startIcon={isSending ? <CircularProgress size={18} /> : <Send />} 
                    disabled={!inputMessage.trim() || isSending} 
                    onClick={sendMessage}
                    sx={{ minWidth: "80px", whiteSpace: "nowrap" }}
                  >
                    {isSending ? "Sending…" : "Send"}
                  </Button>
                  {currentFile && (
                    <Button
                      variant="outlined"
                      startIcon={isRunningWorkflow ? <CircularProgress size={18} /> : <PlayArrow />}
                      disabled={!inputMessage.trim() || isRunningWorkflow || isSending}
                      onClick={runWorkflow}
                      sx={{ 
                        borderColor: "secondary.main", 
                        color: "secondary.main",
                        minWidth: "120px",
                        whiteSpace: "nowrap"
                      }}
                    >
                      {isRunningWorkflow ? "Running…" : "Run Workflow"}
                    </Button>
                  )}
                </Box>
              </Box>
            </Paper>
          )}

          {/* CODE pane (code only) */}
          {tab === "code" && (
            <Paper elevation={0} sx={{ mt: 2, p: 3, bgcolor: "rgba(2,6,23,0.6)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 3 }}>
              <Box sx={{ 
                display: "flex", 
                justifyContent: "space-between", 
                alignItems: { xs: "flex-start", sm: "center" },
                flexDirection: { xs: "column", sm: "row" },
                gap: { xs: 1, sm: 0 },
                mb: 1 
              }}>
                <Typography variant="h6" sx={{ color: "primary.main", fontWeight: 600 }}>Generated ETL Code</Typography>
                <Box sx={{ display: "flex", gap: 0.5, alignItems: "center" }}>
                  <Tooltip title="Load Latest Script">
                    <IconButton 
                      onClick={loadLatestScript}
                      size="small"
                      sx={{ color: "rgba(255,255,255,0.8)" }}
                    >
                      <Refresh fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Copy">
                    <IconButton 
                      onClick={() => copyToClipboard(latestCode)} 
                      disabled={!latestCode}
                      size="small"
                      sx={{ color: "rgba(255,255,255,0.8)" }}
                    >
                      <FileCopy fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Download">
                    <IconButton 
                      onClick={() => downloadCode(latestCode)} 
                      disabled={!latestCode}
                      size="small"
                      sx={{ color: "rgba(255,255,255,0.8)" }}
                    >
                      <Download fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
              </Box>
              {!latestCode ? (
                <Box sx={{ textAlign: "center", py: 4 }}>
                  <Typography sx={{ opacity: 0.7, mb: 2 }}>No code loaded yet.</Typography>
                  <Box sx={{ display: "flex", flexDirection: "column", gap: 1, alignItems: "center" }}>
                    <Typography variant="body2" sx={{ opacity: 0.6 }}>You can:</Typography>
                    <Typography variant="body2" sx={{ opacity: 0.6 }}>• Ask in Chat to generate new ETL code</Typography>
                    <Typography variant="body2" sx={{ opacity: 0.6 }}>• Run the workflow to generate and execute ETL</Typography>
                    <Typography variant="body2" sx={{ opacity: 0.6 }}>• Click the refresh button above to load the latest generated script</Typography>
                  </Box>
                </Box>
              ) : (
                <SyntaxHighlighter
                  language="python"
                  style={vscDarkPlus}
                  customStyle={{
                    maxHeight: 520,
                    overflow: "auto",
                    fontSize: 13,
                    background: "#0f172a",
                    borderRadius: 8,
                    border: "1px solid rgba(56,189,248,0.25)",
                    margin: 0,
                  }}
                  showLineNumbers
                  wrapLines
                >
                  {latestCode}
                </SyntaxHighlighter>
              )}
            </Paper>
          )}

          {/* Profiling pane removed */}

          {/* WORKFLOW pane (populated after workflow) */}
          {tab === "workflow" && (
            <Paper elevation={0} sx={{ mt: 2, p: 3, bgcolor: "rgba(2,6,23,0.6)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 3 }}>
              <Typography variant="h5" sx={{ mb: 1, fontWeight: 600 }}>Workflow</Typography>
              {!workflow ? (
                <Typography sx={{ opacity: 0.7 }}>Run the workflow to see steps and results.</Typography>
              ) : (
                <>
                  <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1.5, mb: 2 }}>
                    <Chip label={`ID: ${workflow.workflow_id ?? "N/A"}`} size="small" />
                    <Chip label={workflow.success ? "Success" : "Failed"} color={workflow.success ? "success" : "error"} size="small" />
                    {workflow.records_inserted !== undefined && <Chip label={`Rows: ${workflow.records_inserted}`} size="small" />}
                    {workflow.script_path && <Chip label={`Script: ${workflow.script_path}`} size="small" />}
                    {workflow.timestamp && <Chip label={new Date(workflow.timestamp).toLocaleString()} size="small" />}
                  </Box>

                  {dag?.length ? (
                    <>
                      <Typography sx={{ fontWeight: 600, mb: 1 }}>Workflow Steps</Typography>
                      <Table size="small" sx={{ borderColor: "rgba(255,255,255,0.08)" }}>
                        <TableHead>
                          <TableRow>
                            <TableCell sx={{ color: "white", fontWeight: 600 }}>Step</TableCell>
                            <TableCell sx={{ color: "white", fontWeight: 600 }}>Status</TableCell>
                            <TableCell sx={{ color: "white", fontWeight: 600 }}>Note</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {dag.map((s, idx) => (
                            <TableRow key={idx} sx={{ '&:nth-of-type(odd)': { bgcolor: 'rgba(255,255,255,0.02)' } }}>
                              <TableCell sx={{ color: "rgba(255,255,255,0.9)", fontWeight: 500 }}>{s.step}</TableCell>
                              <TableCell sx={{ color: "rgba(255,255,255,0.8)" }}>{s.status ?? "-"}</TableCell>
                              <TableCell sx={{ color: "rgba(255,255,255,0.8)" }}>{s.note ?? "-"}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                      <Divider sx={{ my: 2, borderColor: "rgba(255,255,255,0.08)" }} />
                    </>
                  ) : null}

                  {workflow.execution_output && (
                    <>
                      <Typography sx={{ fontWeight: 600, mb: 1 }}>Execution Output</Typography>
                      <Paper variant="outlined" sx={{ p: 2, bgcolor: "rgba(15,23,42,0.7)" }}>
                        <pre style={{ whiteSpace: "pre-wrap", margin: 0 }}>{workflow.execution_output}</pre>
                      </Paper>
                    </>
                  )}

                  {workflow.summary && (
                    <>
                      <Divider sx={{ my: 2, borderColor: "rgba(255,255,255,0.08)" }} />
                      <Typography sx={{ fontWeight: 600, mb: 1 }}>Summary</Typography>
                      <Typography sx={{ whiteSpace: "pre-wrap" }}>{workflow.summary}</Typography>
                    </>
                  )}
                </>
              )}
            </Paper>
          )}
        </Box>
      </Box>
    </Box>
  );
};

export default App;
