import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  IconButton,
  Card,
  CardContent,
  CardActions,
  Grid,
  Alert,
  Snackbar,
  CircularProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import {
  Add as AddIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  Visibility as ViewIcon,
  ExpandMore as ExpandMoreIcon,
  Search as SearchIcon,
  People as PeopleIcon,
  Email as EmailIcon,
} from '@mui/icons-material';

const SearchManager = () => {
  const [searches, setSearches] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedSearch, setSelectedSearch] = useState(null);
  const [searchProfiles, setSearchProfiles] = useState([]);
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [showEditDialog, setShowEditDialog] = useState(false);
  const [showProfilesDialog, setShowProfilesDialog] = useState(false);
  const [newSearch, setNewSearch] = useState({ name: '', description: '', search_url: '' });
  const [editSearch, setEditSearch] = useState({});
  const [statistics, setStatistics] = useState({});
  const [emailSearchLoading, setEmailSearchLoading] = useState(false);

  useEffect(() => {
    fetchSearches();
    fetchStatistics();
  }, []);

  const API_URL = process.env.REACT_APP_API_URL || 'http://143.244.155.153:5000';

  const fetchSearches = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_URL}/api/searches`);
      if (!response.ok) throw new Error('Error al obtener búsquedas');
      const data = await response.json();
      setSearches(data);
    } catch (error) {
      setError('Error al cargar las búsquedas: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchStatistics = async () => {
    try {
      const response = await fetch(`${API_URL}/api/searches/statistics`);
      if (!response.ok) throw new Error('Error al obtener estadísticas');
      const data = await response.json();
      setStatistics(data);
    } catch (error) {
      console.error('Error al obtener estadísticas:', error);
    }
  };

  const fetchSearchProfiles = async (searchId) => {
    try {
      const response = await fetch(`${API_URL}/api/searches/${searchId}/profiles`);
      if (!response.ok) throw new Error('Error al obtener perfiles de la búsqueda');
      const data = await response.json();
      setSearchProfiles(data);
    } catch (error) {
      setError('Error al cargar perfiles: ' + error.message);
    }
  };

  const handleCreateSearch = async () => {
    try {
      const response = await fetch(`${API_URL}/api/searches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newSearch),
      });
      
      if (!response.ok) throw new Error('Error al crear búsqueda');
      
      setShowCreateDialog(false);
      setNewSearch({ name: '', description: '', search_url: '' });
      fetchSearches();
      fetchStatistics();
    } catch (error) {
      setError('Error al crear búsqueda: ' + error.message);
    }
  };

  const handleUpdateSearch = async () => {
    try {
      const response = await fetch(`${API_URL}/api/searches/${editSearch.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(editSearch),
      });
      
      if (!response.ok) throw new Error('Error al actualizar búsqueda');
      
      setShowEditDialog(false);
      setEditSearch({});
      fetchSearches();
      fetchStatistics();
    } catch (error) {
      setError('Error al actualizar búsqueda: ' + error.message);
    }
  };

  const handleDeleteSearch = async (searchId) => {
    if (!window.confirm('¿Está seguro de que desea eliminar esta búsqueda?')) return;
    
    try {
      const response = await fetch(`${API_URL}/api/searches/${searchId}`, {
        method: 'DELETE',
      });
      
      if (!response.ok) throw new Error('Error al eliminar búsqueda');
      
      fetchSearches();
      fetchStatistics();
    } catch (error) {
      setError('Error al eliminar búsqueda: ' + error.message);
    }
  };

  const handleRunEmailSearch = async () => {
    try {
      setEmailSearchLoading(true);
      setError(null);
      
      const response = await fetch(`${API_URL}/api/run-email-search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      
      if (!response.ok) throw new Error('Error al ejecutar búsqueda de emails');
      
      const data = await response.json();
      if (data.status === 'success') {
        setError(null);
        // Recargar estadísticas después de la búsqueda
        fetchStatistics();
      } else {
        throw new Error(data.error || 'Error desconocido');
      }
    } catch (error) {
      setError('Error al ejecutar búsqueda de emails: ' + error.message);
    } finally {
      setEmailSearchLoading(false);
    }
  };

  const handleViewProfiles = (search) => {
    setSelectedSearch(search);
    fetchSearchProfiles(search.id);
    setShowProfilesDialog(true);
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('es-ES', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'active': return 'success';
      case 'inactive': return 'warning';
      case 'completed': return 'info';
      default: return 'default';
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Gestor de Búsquedas
      </Typography>

      {/* Estadísticas */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={2}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total de Búsquedas
              </Typography>
              <Typography variant="h4">
                {statistics.total_searches || 0}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Búsquedas Activas
              </Typography>
              <Typography variant="h4">
                {statistics.active_searches || 0}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Perfiles Únicos
              </Typography>
              <Typography variant="h4">
                {statistics.unique_profiles || 0}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={() => setShowCreateDialog(true)}
                fullWidth
              >
                Nueva Búsqueda
              </Button>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Button
                variant="outlined"
                startIcon={emailSearchLoading ? <CircularProgress size={20} /> : <EmailIcon />}
                onClick={handleRunEmailSearch}
                disabled={emailSearchLoading}
                fullWidth
              >
                {emailSearchLoading ? 'Buscando...' : 'Buscar Emails'}
              </Button>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Lista de búsquedas */}
      <Paper elevation={3} sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          Búsquedas Realizadas
        </Typography>
        
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
            <CircularProgress />
          </Box>
        ) : (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Nombre</TableCell>
                  <TableCell>Descripción</TableCell>
                  <TableCell>Estado</TableCell>
                  <TableCell>Perfiles</TableCell>
                  <TableCell>Creada</TableCell>
                  <TableCell>Acciones</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {searches.map((search) => (
                  <TableRow key={search.id}>
                    <TableCell>
                      <Typography variant="subtitle1" fontWeight="bold">
                        {search.name}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="textSecondary">
                        {search.description || 'Sin descripción'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={search.status}
                        color={getStatusColor(search.status)}
                        size="small"
                      />
                    </TableCell>
                    <TableCell>
                      <Button
                        size="small"
                        startIcon={<PeopleIcon />}
                        onClick={() => handleViewProfiles(search)}
                      >
                        Ver Perfiles
                      </Button>
                    </TableCell>
                    <TableCell>
                      {formatDate(search.created_at)}
                    </TableCell>
                    <TableCell>
                      <IconButton
                        size="small"
                        onClick={() => {
                          setEditSearch(search);
                          setShowEditDialog(true);
                        }}
                      >
                        <EditIcon />
                      </IconButton>
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => handleDeleteSearch(search.id)}
                      >
                        <DeleteIcon />
                      </IconButton>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Paper>

      {/* Diálogo para crear búsqueda */}
      <Dialog open={showCreateDialog} onClose={() => setShowCreateDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Crear Nueva Búsqueda</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Nombre de la búsqueda"
            value={newSearch.name}
            onChange={(e) => setNewSearch({ ...newSearch, name: e.target.value })}
            margin="normal"
            required
          />
          <TextField
            fullWidth
            label="Descripción"
            value={newSearch.description}
            onChange={(e) => setNewSearch({ ...newSearch, description: e.target.value })}
            margin="normal"
            multiline
            rows={3}
          />
          <TextField
            fullWidth
            label="URL de búsqueda"
            value={newSearch.search_url}
            onChange={(e) => setNewSearch({ ...newSearch, search_url: e.target.value })}
            margin="normal"
            required
            helperText="URL de búsqueda de LinkedIn"
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowCreateDialog(false)}>Cancelar</Button>
          <Button onClick={handleCreateSearch} variant="contained">
            Crear
          </Button>
        </DialogActions>
      </Dialog>

      {/* Diálogo para editar búsqueda */}
      <Dialog open={showEditDialog} onClose={() => setShowEditDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Editar Búsqueda</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Nombre de la búsqueda"
            value={editSearch.name || ''}
            onChange={(e) => setEditSearch({ ...editSearch, name: e.target.value })}
            margin="normal"
            required
          />
          <TextField
            fullWidth
            label="Descripción"
            value={editSearch.description || ''}
            onChange={(e) => setEditSearch({ ...editSearch, description: e.target.value })}
            margin="normal"
            multiline
            rows={3}
          />
          <TextField
            fullWidth
            label="Estado"
            value={editSearch.status || ''}
            onChange={(e) => setEditSearch({ ...editSearch, status: e.target.value })}
            margin="normal"
            select
            SelectProps={{ native: true }}
          >
            <option value="active">Activa</option>
            <option value="inactive">Inactiva</option>
            <option value="completed">Completada</option>
          </TextField>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowEditDialog(false)}>Cancelar</Button>
          <Button onClick={handleUpdateSearch} variant="contained">
            Actualizar
          </Button>
        </DialogActions>
      </Dialog>

      {/* Diálogo para ver perfiles de una búsqueda */}
      <Dialog open={showProfilesDialog} onClose={() => setShowProfilesDialog(false)} maxWidth="lg" fullWidth>
        <DialogTitle>
          Perfiles de: {selectedSearch?.name}
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="textSecondary" sx={{ mb: 2 }}>
            {selectedSearch?.description}
          </Typography>
          
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Nombre</TableCell>
                  <TableCell>Título</TableCell>
                  <TableCell>Ubicación</TableCell>
                  <TableCell>Email</TableCell>
                  <TableCell>Teléfono</TableCell>
                  <TableCell>Encontrado</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {searchProfiles.map((profile) => (
                  <TableRow key={profile.id}>
                    <TableCell>{profile.fullName}</TableCell>
                    <TableCell>{profile.headline}</TableCell>
                    <TableCell>{profile.location}</TableCell>
                    <TableCell>{profile.email || 'No disponible'}</TableCell>
                    <TableCell>{profile.mobileNumber || 'No disponible'}</TableCell>
                    <TableCell>{formatDate(profile.found_at)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          
          {searchProfiles.length === 0 && (
            <Typography variant="body2" color="textSecondary" sx={{ textAlign: 'center', py: 3 }}>
              No se encontraron perfiles para esta búsqueda
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowProfilesDialog(false)}>Cerrar</Button>
        </DialogActions>
      </Dialog>

      {/* Snackbar para errores */}
      <Snackbar
        open={!!error}
        autoHideDuration={6000}
        onClose={() => setError(null)}
      >
        <Alert onClose={() => setError(null)} severity="error">
          {error}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default SearchManager; 