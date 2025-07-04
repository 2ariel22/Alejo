import React, { useState, useEffect } from 'react';
import {
  Container,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  TextField,
  Box,
  Chip,
  IconButton,
  Tooltip,
  CircularProgress,
  Alert,
  Snackbar,
  Tabs,
  Tab,
  Checkbox,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  Divider
} from '@mui/material';
import {
  LinkedIn as LinkedInIcon,
  Email as EmailIcon,
  Phone as PhoneIcon,
  Search as SearchIcon,
  Refresh as RefreshIcon,
  Settings as SettingsIcon,
  List as ListIcon,
  CloudUpload as CloudUploadIcon
} from '@mui/icons-material';
import axios from 'axios';
import ScraperConfig from './components/ScraperConfig';
import SearchTabs from './components/SearchTabs';

function App() {
  const [profiles, setProfiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredProfiles, setFilteredProfiles] = useState([]);
  const [error, setError] = useState(null);
  const [status, setStatus] = useState(null);
  const [activeTab, setActiveTab] = useState(0);
  const [selectedContacts, setSelectedContacts] = useState([]);
  const [selectAll, setSelectAll] = useState(false);
  const [hubspotDialogOpen, setHubspotDialogOpen] = useState(false);
  const [hubspotResult, setHubspotResult] = useState(null);
  const [sendingToHubspot, setSendingToHubspot] = useState(false);
  const [isLoggedIn, setIsLoggedIn] = useState(!!sessionStorage.getItem('token'));
  const [loginLoading, setLoginLoading] = useState(false);
  const [loginError, setLoginError] = useState(null);
  const [loginUser, setLoginUser] = useState('');
  const [loginPass, setLoginPass] = useState('');

  useEffect(() => {
    checkStatus();
    fetchProfiles();
  }, []);

  useEffect(() => {
    if (!Array.isArray(profiles)) {
      console.error('Profiles is not an array:', profiles);
      setFilteredProfiles([]);
      return;
    }

    const filtered = profiles.filter(profile => {
      if (!profile) return false;
      
      const searchLower = searchTerm.toLowerCase();
      return (
        (profile.fullName?.toLowerCase() || '').includes(searchLower) ||
        (profile.headline?.toLowerCase() || '').includes(searchLower) ||
        (profile.location?.toLowerCase() || '').includes(searchLower) ||
        (profile.email?.toLowerCase() || '').includes(searchLower) ||
        (profile.mobileNumber?.toLowerCase() || '').includes(searchLower)
      );
    });
    setFilteredProfiles(filtered);
    
    // Actualizar selección cuando cambian los perfiles filtrados
    setSelectedContacts(prev => {
      const validSelected = prev.filter(contact => 
        filtered.some(profile => profile.id === contact.id)
      );
      return validSelected;
    });
    
    // Actualizar estado de "seleccionar todo"
    setSelectAll(selectedContacts.length === filtered.length && filtered.length > 0);
  }, [searchTerm, profiles]);

  const API_URL = process.env.REACT_APP_API_URL || 'http://143.244.155.153:5000';

  const checkStatus = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/status`);
      console.log('Status response:', response.data);
      setStatus(response.data);
    } catch (error) {
      console.error('Error checking status:', error);
    }
  };

  const fetchProfiles = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await axios.get(`${API_URL}/api/profiles`);
      console.log('Profiles response:', response.data);
      
      // Ensure we have an array of profiles
      const profilesData = Array.isArray(response.data) ? response.data : [];
      console.log('Processed profiles data:', profilesData);
      
      setProfiles(profilesData);
      setFilteredProfiles(profilesData);
    } catch (error) {
      console.error('Error fetching profiles:', error);
      setError(error.response?.data?.error || 'Error al cargar los perfiles');
      setProfiles([]);
      setFilteredProfiles([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (event) => {
    setSearchTerm(event.target.value);
  };

  const getStatusChip = (profile) => {
    if (!profile) return null;
    
    if (profile.email || profile.mobileNumber) {
      return <Chip label="Contacto Encontrado" color="success" size="small" />;
    }
    if (profile.email_checked) {
      return <Chip label="Sin Contacto" color="error" size="small" />;
    }
    return <Chip label="Pendiente" color="warning" size="small" />;
  };

  const handleCloseError = () => {
    setError(null);
  };

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  const handleScrapingComplete = () => {
    fetchProfiles();
    setActiveTab(0); // Cambiar a la pestaña de perfiles
  };

  const handleSelectContact = (contact) => {
    setSelectedContacts(prev => {
      const isSelected = prev.some(c => c.id === contact.id);
      if (isSelected) {
        return prev.filter(c => c.id !== contact.id);
      } else {
        return [...prev, contact];
      }
    });
  };

  const handleSelectAll = () => {
    if (selectAll) {
      setSelectedContacts([]);
      setSelectAll(false);
    } else {
      setSelectedContacts([...filteredProfiles]);
      setSelectAll(true);
    }
  };

  const handleSendToHubspot = async () => {
    console.log('selectedContacts:', selectedContacts);
    console.log('selectedContacts length:', selectedContacts.length);
    
    if (selectedContacts.length === 0) {
      setError('No hay contactos seleccionados');
      return;
    }

    setSendingToHubspot(true);
    setHubspotDialogOpen(true);
    setHubspotResult(null);

    const requestData = {
      contacts: selectedContacts
    };
    
    console.log('Enviando datos a HubSpot:', requestData);

    try {
      const response = await axios.post(`${API_URL}/api/send-to-hubspot`, requestData);

      setHubspotResult(response.data);
    } catch (error) {
      console.error('Error enviando a HubSpot:', error);
      setHubspotResult({
        success: false,
        message: error.response?.data?.error || 'Error al enviar contactos a HubSpot'
      });
    } finally {
      setSendingToHubspot(false);
    }
  };

  const handleCloseHubspotDialog = () => {
    setHubspotDialogOpen(false);
    setHubspotResult(null);
    if (hubspotResult?.success) {
      setSelectedContacts([]);
      setSelectAll(false);
    }
  };

  // Función para login
  const handleLogin = async (e) => {
    e.preventDefault();
    setLoginLoading(true);
    setLoginError(null);
    try {
      const res = await axios.post(`${API_URL}/api/login`, {
        username: loginUser,
        password: loginPass
      });
      if (res.data.success) {
        sessionStorage.setItem('token', res.data.token);
        setIsLoggedIn(true);
      } else {
        setLoginError('Credenciales incorrectas');
      }
    } catch (err) {
      setLoginError('Error de conexión o credenciales incorrectas');
    } finally {
      setLoginLoading(false);
    }
  };

  // Función para logout
  const handleLogout = () => {
    sessionStorage.removeItem('token');
    setIsLoggedIn(false);
  };

  // Interceptor para agregar token a peticiones protegidas
  useEffect(() => {
    const reqInterceptor = axios.interceptors.request.use(config => {
      const token = sessionStorage.getItem('token');
      if (token && config.url.includes('/api/send-to-hubspot')) {
        config.headers['Authorization'] = token;
      }
      return config;
    });
    return () => axios.interceptors.request.eject(reqInterceptor);
  }, []);

  if (!isLoggedIn) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
        <Paper sx={{ p: 4, minWidth: 320 }}>
          <Typography variant="h5" gutterBottom>Iniciar sesión</Typography>
          <form onSubmit={handleLogin}>
            <TextField
              label="Usuario"
              fullWidth
              margin="normal"
              value={loginUser}
              onChange={e => setLoginUser(e.target.value)}
              autoFocus
            />
            <TextField
              label="Contraseña"
              type="password"
              fullWidth
              margin="normal"
              value={loginPass}
              onChange={e => setLoginPass(e.target.value)}
            />
            {loginError && <Alert severity="error" sx={{ mt: 2 }}>{loginError}</Alert>}
            <Button
              type="submit"
              variant="contained"
              color="primary"
              fullWidth
              sx={{ mt: 2 }}
              disabled={loginLoading}
            >
              {loginLoading ? 'Ingresando...' : 'Ingresar'}
            </Button>
          </form>
        </Paper>
      </Box>
    );
  }

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1">
          Perfiles de LinkedIn
        </Typography>
        <Tooltip title="Actualizar datos">
          <IconButton onClick={fetchProfiles} color="primary">
            <RefreshIcon />
          </IconButton>
        </Tooltip>
        <Button color="secondary" onClick={handleLogout}>Cerrar sesión</Button>
      </Box>

      <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
        <Tab icon={<ListIcon />} label="Perfiles" />
        <Tab icon={<SettingsIcon />} label="Configuración" />
      </Tabs>

      {activeTab === 0 ? (
        <>
          {/* Barra de pestañas de búsquedas - solo en la vista de Perfiles */}
          <SearchTabs
            selectedContacts={selectedContacts}
            setSelectedContacts={setSelectedContacts}
          />

          <Box sx={{ mb: 3 }}>
            <TextField
              fullWidth
              variant="outlined"
              placeholder="Buscar por nombre, cargo, ubicación, email o teléfono..."
              value={searchTerm}
              onChange={handleSearch}
              InputProps={{
                startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
              }}
            />
          </Box>

          {/* Tabla de perfiles con selección y botón HubSpot */}
          {!Array.isArray(filteredProfiles) || filteredProfiles.length === 0 ? (
            <Alert severity="info">
              No hay perfiles para mostrar. Ejecuta el script de Python para obtener perfiles.
            </Alert>
          ) : (
            <>
              {selectedContacts.length > 0 && (
                <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Typography variant="body2" color="text.secondary">
                    {selectedContacts.length} contacto(s) seleccionado(s)
                  </Typography>
                  <Button
                    variant="contained"
                    color="primary"
                    startIcon={<CloudUploadIcon />}
                    onClick={handleSendToHubspot}
                    disabled={sendingToHubspot}
                  >
                    {sendingToHubspot ? 'Enviando...' : 'Enviar a HubSpot'}
                  </Button>
                </Box>
              )}
              <TableContainer component={Paper}>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell padding="checkbox">
                        <Checkbox
                          checked={selectAll}
                          indeterminate={selectedContacts.length > 0 && selectedContacts.length < filteredProfiles.length}
                          onChange={handleSelectAll}
                        />
                      </TableCell>
                      <TableCell>Nombre</TableCell>
                      <TableCell>Cargo</TableCell>
                      <TableCell>Ubicación</TableCell>
                      <TableCell>Email</TableCell>
                      <TableCell>Teléfono</TableCell>
                      <TableCell>Estado</TableCell>
                      <TableCell>Acciones</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredProfiles.map((profile, index) => {
                      const isSelected = selectedContacts.some(c => c.id === profile.id);
                      return (
                        <TableRow key={profile?.id || index}>
                          <TableCell padding="checkbox">
                            <Checkbox
                              checked={isSelected}
                              onChange={() => handleSelectContact(profile)}
                            />
                          </TableCell>
                          <TableCell>
                            <Typography variant="subtitle2">{profile?.fullName || 'N/A'}</Typography>
                          </TableCell>
                          <TableCell>{profile?.headline || 'N/A'}</TableCell>
                          <TableCell>{profile?.location || 'N/A'}</TableCell>
                          <TableCell>
                            {profile?.email && profile.email !== '' ? (
                              <Tooltip title={profile.email}>
                                <span>{profile.email}</span>
                              </Tooltip>
                            ) : 'No disponible'}
                          </TableCell>
                          <TableCell>
                            {profile?.mobileNumber && profile.mobileNumber !== '' ? (
                              <Tooltip title={profile.mobileNumber}>
                                <span>{profile.mobileNumber}</span>
                              </Tooltip>
                            ) : 'No disponible'}
                          </TableCell>
                          <TableCell>{getStatusChip(profile)}</TableCell>
                          <TableCell>
                            {profile?.profileUrl && (
                              <Tooltip title="Ver perfil de LinkedIn">
                                <IconButton
                                  href={profile.profileUrl}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  size="small"
                                >
                                  <LinkedInIcon color="primary" />
                                </IconButton>
                              </Tooltip>
                            )}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
            </>
          )}

          {/* Diálogo de resultado de HubSpot */}
          <Dialog open={hubspotDialogOpen} onClose={handleCloseHubspotDialog} maxWidth="sm" fullWidth>
            <DialogTitle>Resultado de envío a HubSpot</DialogTitle>
            <DialogContent>
              {hubspotResult ? (
                hubspotResult.success ? (
                  <Alert severity="success">{hubspotResult.message}</Alert>
                ) : (
                  <Alert severity="error">{hubspotResult.message}</Alert>
                )
              ) : (
                <Box display="flex" justifyContent="center" alignItems="center" py={3}>
                  <CircularProgress />
                </Box>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseHubspotDialog}>Cerrar</Button>
            </DialogActions>
          </Dialog>
        </>
      ) : (
        <ScraperConfig onScrapingComplete={handleScrapingComplete} />
      )}

      <Snackbar
        open={!!error}
        autoHideDuration={6000}
        onClose={handleCloseError}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={handleCloseError} severity="error" sx={{ width: '100%' }}>
          {error}
        </Alert>
      </Snackbar>
    </Container>
  );
}

export default App; 