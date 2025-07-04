import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  Button,
  Typography,
  Paper,
  CircularProgress,
  Alert,
  Snackbar,
  LinearProgress,
} from '@mui/material';
import { PlayArrow as PlayArrowIcon } from '@mui/icons-material';

const ScraperConfig = ({ onScrapingComplete }) => {
  const [urlsearch, setUrlsearch] = useState('');
  const [searchName, setSearchName] = useState('');
  const [searchDescription, setSearchDescription] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [status, setStatus] = useState(null);
  const [pollingInterval, setPollingInterval] = useState(null);
  const [processId, setProcessId] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const MAX_RETRIES = 3;

  useEffect(() => {
    // Limpiar el intervalo cuando el componente se desmonte
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval);
      }
    };
  }, [pollingInterval]);

  const API_URL = process.env.REACT_APP_API_URL || 'http://143.244.155.153:5000';

  const checkScrapingStatus = async () => {
    try {
      const response = await fetch(`${API_URL}/api/scraping-status`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      
      if (data.status === 'success') {
        // Limpiar el intervalo y notificar que el scraping ha terminado
        clearInterval(pollingInterval);
        setPollingInterval(null);
        setLoading(false);
        setStatus('success');
        setProcessId(null);
        setRetryCount(0);
        if (onScrapingComplete) {
          onScrapingComplete();
        }
      } else if (data.status === 'error') {
        // Limpiar el intervalo y mostrar el error
        clearInterval(pollingInterval);
        setPollingInterval(null);
        setLoading(false);
        setError(data.error || 'Error durante el scraping');
        setProcessId(null);
        setRetryCount(0);
      }
      // Si el estado es 'running', continuamos con el polling
    } catch (error) {
      console.error('Error checking scraping status:', error);
      // Incrementar el contador de reintentos
      setRetryCount(prev => prev + 1);
      
      // Si excedemos el número máximo de reintentos, detener el polling
      if (retryCount >= MAX_RETRIES) {
        clearInterval(pollingInterval);
        setPollingInterval(null);
        setLoading(false);
        setError('Error al verificar el estado del scraping. Por favor, intente nuevamente.');
        setProcessId(null);
        setRetryCount(0);
      }
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setStatus(null);
    setRetryCount(0);

    try {
      const response = await fetch(`${API_URL}/api/run-scraper`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
          urlsearch,
          search_name: searchName,
          search_description: searchDescription
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();

      if (data.error) {
        setError(data.error);
        setLoading(false);
      } else if (data.status === 'started') {
        setProcessId(data.process_id);
        // Iniciar el polling del estado
        const interval = setInterval(checkScrapingStatus, 5000); // Verificar cada 5 segundos
        setPollingInterval(interval);
      }
    } catch (error) {
      console.error('Error starting scraping:', error);
      setError('Error al iniciar el scraping. Por favor, intente nuevamente.');
      setLoading(false);
    }
  };

  return (
    <Paper elevation={3} sx={{ p: 3, maxWidth: 600, mx: 'auto', mt: 4 }}>
      <Typography variant="h5" gutterBottom>
        Configuración del Scraper
      </Typography>
      
      <form onSubmit={handleSubmit}>
        <TextField
          fullWidth
          label="Nombre de la búsqueda"
          value={searchName}
          onChange={(e) => setSearchName(e.target.value)}
          margin="normal"
          required
          disabled={loading}
          helperText="Ingrese un nombre descriptivo para esta búsqueda"
        />
        
        <TextField
          fullWidth
          label="Descripción de la búsqueda"
          value={searchDescription}
          onChange={(e) => setSearchDescription(e.target.value)}
          margin="normal"
          disabled={loading}
          multiline
          rows={3}
          helperText="Opcional: Describa el propósito o criterios de esta búsqueda"
        />
        
        <TextField
          fullWidth
          label="URL de búsqueda de LinkedIn"
          value={urlsearch}
          onChange={(e) => setUrlsearch(e.target.value)}
          margin="normal"
          required
          disabled={loading}
          helperText="Ingrese la URL de búsqueda de LinkedIn que desea scrapear"
        />
        
        <Box sx={{ mt: 2, display: 'flex', alignItems: 'center', gap: 2 }}>
          <Button
            type="submit"
            variant="contained"
            color="primary"
            disabled={loading || !urlsearch || !searchName}
            startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <PlayArrowIcon />}
          >
            {loading ? 'Ejecutando...' : 'Ejecutar Scraping'}
          </Button>
          
          {loading && (
            <Typography variant="body2" color="textSecondary">
              El proceso puede tomar varios minutos...
            </Typography>
          )}
        </Box>
      </form>

      {loading && (
        <Box sx={{ mt: 2 }}>
          <LinearProgress />
          <Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
            Proceso ID: {processId}
          </Typography>
        </Box>
      )}

      {error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          {error}
        </Alert>
      )}

      {status === 'success' && (
        <Alert severity="success" sx={{ mt: 2 }}>
          Scraping completado exitosamente
        </Alert>
      )}

      <Snackbar
        open={!!error || status === 'success'}
        autoHideDuration={6000}
        onClose={() => {
          setError(null);
          setStatus(null);
        }}
      >
        <Alert
          onClose={() => {
            setError(null);
            setStatus(null);
          }}
          severity={error ? 'error' : 'success'}
          sx={{ width: '100%' }}
        >
          {error || 'Scraping completado exitosamente'}
        </Alert>
      </Snackbar>
    </Paper>
  );
};

export default ScraperConfig; 