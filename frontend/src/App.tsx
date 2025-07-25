import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Container,
  Paper,
  Typography,
  TextField,
  Button,
  Chip,
  Alert,
  CircularProgress,
  Divider,
  Card,
  CardContent,
  IconButton,
  Tooltip
} from '@mui/material';
import {
  CloudUpload,
  Send,
  FileCopy,
  Download,
  Storage,
  Clear
} from '@mui/icons-material';
import { useDropzone } from 'react-dropzone';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import axios from 'axios';

// Configure axios base URL
axios.defaults.baseURL = 'http://localhost:8000';

interface Message {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  etlCode?: string;
  fileInfo?: any;
}

interface FileInfo {
  s3_url: string;
  filename: string;
  original_filename: string;
  content_type: string;
  size: number;
}

// Format message content for better readability
const formatMessageContent = (content: string): React.ReactNode => {
  // Split content into sections based on common patterns
  const lines = content.split('\n');
  const formattedLines: React.ReactNode[] = [];
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    // Skip empty lines but add spacing
    if (line.trim() === '') {
      formattedLines.push(<br key={`br-${i}`} />);
      continue;
    }
    
    // Headers (lines ending with colon or all caps)
    if (line.endsWith(':') || (line === line.toUpperCase() && line.length > 3)) {
      formattedLines.push(
        <Typography key={i} variant="subtitle2" sx={{ 
          fontWeight: 600, 
          color: 'primary.light', 
          mt: i > 0 ? 2 : 0,
          mb: 1 
        }}>
          {line}
        </Typography>
      );
    }
    // List items (starting with -, *, or numbers)
    else if (line.match(/^\s*[-*•]\s/) || line.match(/^\s*\d+\.\s/)) {
      formattedLines.push(
        <Typography key={i} variant="body2" sx={{ 
          ml: 2, 
          mb: 0.5,
          '&:before': {
            content: '"•"',
            color: 'primary.main',
            fontWeight: 'bold',
            marginRight: '8px'
          }
        }}>
          {line.replace(/^\s*[-*•]\s/, '').replace(/^\s*\d+\.\s/, '')}
        </Typography>
      );
    }
    // Code or file paths (containing special characters)
    else if (line.includes('s3://') || line.includes('.py') || line.includes('ERROR:') || line.includes('INFO:')) {
      formattedLines.push(
        <Typography key={i} variant="body2" sx={{ 
          fontFamily: 'monospace',
          backgroundColor: 'rgba(0, 188, 212, 0.1)',
          padding: '4px 8px',
          borderRadius: '4px',
          fontSize: '0.85rem',
          my: 0.5,
          wordBreak: 'break-all'
        }}>
          {line}
        </Typography>
      );
    }
    // Regular text
    else {
      formattedLines.push(
        <Typography key={i} variant="body1" sx={{ mb: 0.5, lineHeight: 1.6 }}>
          {line}
        </Typography>
      );
    }
  }
  
  return <>{formattedLines}</>;
};

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      type: 'assistant',
      content: 'Hello! I can help you upload files to S3 and generate ETL code for Snowflake ingestion. Please upload a file and describe what you\'d like to do with it.',
      timestamp: new Date()
    }
  ]);
  const [inputMessage, setInputMessage] = useState('');
  const [currentFile, setCurrentFile] = useState<FileInfo | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [isSending, setIsSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Check if chat has started (more than just the initial welcome message)
  const isChatStarted = messages.length > 1;

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const onDrop = async (acceptedFiles: File[]) => {
    if (acceptedFiles.length === 0) return;

    const file = acceptedFiles[0];
    setIsUploading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await axios.post('/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      setCurrentFile(response.data);
      
      const newMessage: Message = {
        id: Date.now().toString(),
        type: 'user',
        content: `Uploaded file: ${file.name}`,
        timestamp: new Date(),
        fileInfo: response.data
      };

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: 'assistant',
        content: `File successfully uploaded to S3: ${response.data.s3_url}\n\nFile details:\n- Size: ${(response.data.size / 1024).toFixed(2)} KB\n- Type: ${response.data.content_type}\n\nNow you can describe what ETL operations you'd like to perform on this file.`,
        timestamp: new Date()
      };

      setMessages(prev => [...prev, newMessage, assistantMessage]);
    } catch (err: any) {
      console.error('Upload error:', err);
      let errorMessage = 'Failed to upload file. Please try again.';
      
      if (err.response) {
        // Server responded with error status
        errorMessage = `Upload failed: ${err.response.data?.detail || err.response.statusText}`;
      } else if (err.request) {
        // Request was made but no response received
        errorMessage = 'Upload failed: Cannot connect to server. Make sure the backend is running on port 8000.';
      } else {
        // Something else happened
        errorMessage = `Upload failed: ${err.message}`;
      }
      
      setError(errorMessage);
    } finally {
      setIsUploading(false);
    }
  };

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
      'application/vnd.ms-excel': ['.xls'],
      'text/plain': ['.txt'],
      'application/parquet': ['.parquet']
    },
    multiple: false
  });

  const sendMessage = async () => {
    if (!inputMessage.trim()) return;

    setIsSending(true);
    setError(null);

    const userMessage: Message = {
      id: Date.now().toString(),
      type: 'user',
      content: inputMessage,
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputMessage('');

    try {
      const response = await axios.post('/chat', {
        message: inputMessage,
        file_url: currentFile?.s3_url,
        file_name: currentFile?.original_filename
      });

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: 'assistant',
        content: response.data.response,
        timestamp: new Date(),
        etlCode: response.data.etl_code
      };

      setMessages(prev => [...prev, assistantMessage]);
    } catch (err: any) {
      console.error('Chat error:', err);
      let errorMessage = 'Failed to send message. Please try again.';
      
      if (err.response) {
        errorMessage = `Chat failed: ${err.response.data?.detail || err.response.statusText}`;
      } else if (err.request) {
        errorMessage = 'Chat failed: Cannot connect to server. Make sure the backend is running on port 8000.';
      } else {
        errorMessage = `Chat failed: ${err.message}`;
      }
      
      setError(errorMessage);
    } finally {
      setIsSending(false);
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const downloadCode = (code: string, filename: string = 'etl_code.py') => {
    const blob = new Blob([code], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const clearChat = () => {
    setMessages([
      {
        id: '1',
        type: 'assistant',
        content: 'Hello! I can help you upload files to S3 and generate ETL code for Snowflake ingestion. Please upload a file and describe what you\'d like to do with it.',
        timestamp: new Date()
      }
    ]);
    setCurrentFile(null);
    setError(null);
  };

  return (
    <Container maxWidth="xl" sx={{ py: 3 }}>
      <Paper 
        elevation={3} 
        sx={{ 
          height: '90vh', 
          display: 'flex', 
          flexDirection: 'column',
          backgroundColor: 'background.paper',
          border: '1px solid rgba(0, 188, 212, 0.2)',
          boxShadow: '0 8px 32px rgba(0, 188, 212, 0.1)',
          overflow: 'hidden'
        }}
      >
        {/* Header */}
        <Box sx={{ 
          p: isChatStarted ? 2 : 3, 
          background: 'linear-gradient(135deg, #00bcd4 0%, #00838f 100%)',
          color: 'white',
          position: 'relative',
          overflow: 'hidden',
          transition: 'padding 0.3s ease-in-out',
          '&::before': {
            content: '""',
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0, 229, 255, 0.1)',
            backgroundImage: 'radial-gradient(circle at 20% 50%, rgba(0, 229, 255, 0.2) 0%, transparent 50%), radial-gradient(circle at 80% 20%, rgba(0, 229, 255, 0.15) 0%, transparent 50%)',
          }
        }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <Box>
              <Typography 
                variant={isChatStarted ? "h5" : "h4"} 
                component="h1" 
                gutterBottom={!isChatStarted} 
                sx={{ 
                  position: 'relative', 
                  zIndex: 1, 
                  fontWeight: 600,
                  transition: 'font-size 0.3s ease-in-out',
                  mb: isChatStarted ? 0 : 1
                }}
              >
                LLM ETL Chat Application
              </Typography>
              {!isChatStarted && (
                <Typography variant="subtitle1" sx={{ position: 'relative', zIndex: 1, opacity: 0.9 }}>
                  Upload files to S3 and generate ETL code for Snowflake ingestion
                </Typography>
              )}
            </Box>
            <Tooltip title="Clear chat history">
              <IconButton
                onClick={clearChat}
                sx={{
                  position: 'relative',
                  zIndex: 1,
                  color: 'white',
                  backgroundColor: 'rgba(255, 255, 255, 0.1)',
                  '&:hover': {
                    backgroundColor: 'rgba(255, 255, 255, 0.2)',
                    transform: 'scale(1.05)',
                  },
                  transition: 'all 0.2s ease-in-out',
                }}
              >
                <Clear />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>

        {/* File Upload Status */}
        {currentFile && (
          <Alert 
            severity="success" 
            sx={{ 
              m: 2,
              backgroundColor: 'rgba(0, 188, 212, 0.1)',
              border: '1px solid rgba(0, 188, 212, 0.3)',
              '& .MuiAlert-icon': {
                color: 'primary.main'
              }
            }} 
            icon={<Storage />}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
              <Typography sx={{ fontWeight: 500 }}>
                <strong>File uploaded:</strong> {currentFile.original_filename}
              </Typography>
              <Chip 
                label={currentFile.content_type} 
                size="small" 
                sx={{ 
                  backgroundColor: 'rgba(0, 188, 212, 0.2)',
                  color: 'primary.main'
                }} 
              />
              <Chip 
                label={`${(currentFile.size / 1024).toFixed(2)} KB`} 
                size="small"
                sx={{ 
                  backgroundColor: 'rgba(0, 188, 212, 0.2)',
                  color: 'primary.main'
                }} 
              />
              <Chip 
                label="Ready for ETL" 
                size="small"
                sx={{ 
                  backgroundColor: 'rgba(76, 175, 80, 0.2)',
                  color: '#4caf50'
                }} 
              />
            </Box>
          </Alert>
        )}

        {/* Error Display */}
        {error && (
          <Alert severity="error" sx={{ m: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        {/* Messages Area */}
        <Box sx={{ 
          flex: 1, 
          overflow: 'auto', 
          p: 3,
          backgroundColor: 'rgba(0, 0, 0, 0.2)', // Subtle background
          '&::-webkit-scrollbar': {
            width: '6px',
          },
          '&::-webkit-scrollbar-track': {
            backgroundColor: 'rgba(255, 255, 255, 0.1)',
            borderRadius: '3px',
          },
          '&::-webkit-scrollbar-thumb': {
            backgroundColor: 'rgba(0, 188, 212, 0.5)',
            borderRadius: '3px',
            '&:hover': {
              backgroundColor: 'rgba(0, 188, 212, 0.7)',
            },
          },
        }}>
          {messages.map((message) => (
            <Box key={message.id} sx={{ mb: 3 }}>
              <Card
                sx={{
                  maxWidth: '80%',
                  ml: message.type === 'user' ? 'auto' : 0,
                  mr: message.type === 'assistant' ? 'auto' : 0,
                  bgcolor: message.type === 'user' 
                    ? 'rgba(0, 188, 212, 0.15)' 
                    : 'rgba(255, 255, 255, 0.05)',
                  border: message.type === 'user' 
                    ? '1px solid rgba(0, 188, 212, 0.3)' 
                    : '1px solid rgba(255, 255, 255, 0.1)',
                  boxShadow: message.type === 'user'
                    ? '0 4px 12px rgba(0, 188, 212, 0.1)'
                    : '0 4px 12px rgba(0, 0, 0, 0.3)'
                }}
              >
                <CardContent sx={{ p: 3 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Chip 
                      label={message.type === 'user' ? 'You' : 'Assistant'}
                      size="small"
                      variant="outlined"
                      sx={{ 
                        borderColor: message.type === 'user' ? 'primary.main' : 'grey.600',
                        color: message.type === 'user' ? 'primary.main' : 'text.secondary'
                      }}
                    />
                    <Typography variant="caption" color="textSecondary" sx={{ fontSize: '0.75rem' }}>
                      {message.timestamp.toLocaleTimeString()}
                    </Typography>
                  </Box>
                  <Box sx={{ 
                    '& > *:last-child': { mb: 0 } // Remove margin from last element
                  }}>
                    {formatMessageContent(message.content)}
                  </Box>
                  
                  {message.etlCode && (
                    <Box sx={{ mt: 3 }}>
                      <Divider sx={{ my: 2, borderColor: 'rgba(0, 188, 212, 0.3)' }} />
                      <Box sx={{ 
                        display: 'flex', 
                        justifyContent: 'space-between', 
                        alignItems: 'center', 
                        mb: 2,
                        p: 2,
                        backgroundColor: 'rgba(0, 188, 212, 0.05)',
                        borderRadius: '8px',
                        border: '1px solid rgba(0, 188, 212, 0.2)'
                      }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography variant="h6" sx={{ color: 'primary.main', fontWeight: 600 }}>
                            Generated ETL Code
                          </Typography>
                          <Chip 
                            label="Python" 
                            size="small" 
                            sx={{ 
                              backgroundColor: 'rgba(0, 188, 212, 0.2)',
                              color: 'primary.main',
                              fontSize: '0.7rem'
                            }} 
                          />
                        </Box>
                        <Box>
                          <Tooltip title="Copy to clipboard">
                            <IconButton 
                              onClick={() => copyToClipboard(message.etlCode!)}
                              sx={{ 
                                color: 'primary.main',
                                '&:hover': { 
                                  backgroundColor: 'rgba(0, 188, 212, 0.1)',
                                  transform: 'scale(1.1)'
                                }
                              }}
                            >
                              <FileCopy />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Download as file">
                            <IconButton 
                              onClick={() => downloadCode(message.etlCode!)}
                              sx={{ 
                                color: 'primary.main',
                                '&:hover': { 
                                  backgroundColor: 'rgba(0, 188, 212, 0.1)',
                                  transform: 'scale(1.1)'
                                }
                              }}
                            >
                              <Download />
                            </IconButton>
                          </Tooltip>
                        </Box>
                      </Box>
                      <SyntaxHighlighter
                        language="python"
                        style={vscDarkPlus}
                        customStyle={{
                          maxHeight: '500px',
                          overflow: 'auto',
                          fontSize: '13px',
                          lineHeight: '1.5',
                          backgroundColor: '#1e1e1e',
                          borderRadius: '8px',
                          border: '1px solid rgba(0, 188, 212, 0.2)',
                          boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
                          padding: '16px',
                          margin: 0
                        }}
                        showLineNumbers={true}
                        wrapLines={true}
                      >
                        {message.etlCode}
                      </SyntaxHighlighter>
                    </Box>
                  )}
                </CardContent>
              </Card>
            </Box>
          ))}
          
          {/* Loading indicator when assistant is thinking */}
          {isSending && (
            <Box sx={{ mb: 3 }}>
              <Card
                sx={{
                  maxWidth: '80%',
                  mr: 'auto',
                  bgcolor: 'rgba(255, 255, 255, 0.05)',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)'
                }}
              >
                <CardContent sx={{ p: 3 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <CircularProgress size={20} sx={{ color: 'primary.main' }} />
                    <Typography variant="body1" sx={{ fontStyle: 'italic', color: 'text.secondary' }}>
                      Assistant is generating response...
                    </Typography>
                  </Box>
                </CardContent>
              </Card>
            </Box>
          )}
          
          <div ref={messagesEndRef} />
        </Box>

        {/* Input Area */}
        <Box sx={{ p: 3, borderTop: 1, borderColor: 'divider' }}>
          {/* File Upload */}
          <Box
            {...getRootProps()}
            sx={{
              p: 2,
              mb: 2,
              border: 2,
              borderColor: isDragActive ? 'primary.main' : 'grey.600',
              borderStyle: 'dashed',
              borderRadius: 2,
              textAlign: 'center',
              cursor: 'pointer',
              bgcolor: isDragActive ? 'rgba(0, 188, 212, 0.1)' : 'rgba(255, 255, 255, 0.02)',
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                borderColor: 'primary.main',
                bgcolor: 'rgba(0, 188, 212, 0.05)',
                transform: 'translateY(-1px)',
                boxShadow: '0 4px 12px rgba(0, 188, 212, 0.15)'
              }
            }}
          >
            <input {...getInputProps()} />
            {isUploading ? (
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 2 }}>
                <CircularProgress size={24} />
                <Typography>Uploading...</Typography>
              </Box>
            ) : (
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 2 }}>
                <CloudUpload color="primary" />
                <Typography>
                  {isDragActive
                    ? 'Drop the file here...'
                    : 'Drag & drop a file here, or click to select (CSV, JSON, Excel, Parquet)'}
                </Typography>
              </Box>
            )}
          </Box>

          {/* Message Input */}
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'flex-end' }}>
            <TextField
              fullWidth
              multiline
              maxRows={4}
              minRows={1}
              placeholder="Describe your ETL requirements..."
              value={inputMessage}
              onChange={(e) => setInputMessage(e.target.value)}
              onKeyPress={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  sendMessage();
                }
              }}
              disabled={isSending}
              variant="outlined"
              sx={{
                '& .MuiOutlinedInput-root': {
                  backgroundColor: 'rgba(255, 255, 255, 0.05)',
                  '&:hover': {
                    backgroundColor: 'rgba(255, 255, 255, 0.08)',
                  },
                  '&.Mui-focused': {
                    backgroundColor: 'rgba(255, 255, 255, 0.08)',
                  },
                },
              }}
            />
            <Button
              variant="contained"
              startIcon={isSending ? <CircularProgress size={20} /> : <Send />}
              onClick={sendMessage}
              disabled={!inputMessage.trim() || isSending}
              sx={{ 
                minWidth: 120,
                height: 'fit-content',
                py: 1.5,
                boxShadow: '0 4px 12px rgba(0, 188, 212, 0.3)',
                '&:hover': {
                  boxShadow: '0 6px 16px rgba(0, 188, 212, 0.4)',
                }
              }}
            >
              {isSending ? 'Sending...' : 'Send'}
            </Button>
          </Box>
        </Box>
      </Paper>
    </Container>
  );
};

export default App;
