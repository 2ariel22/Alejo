import React, { useState, useEffect } from 'react';
import {
  Box,
  Tabs,
  Tab,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  CircularProgress,
  Checkbox,
  Button,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tooltip,
  Chip,
  IconButton,
} from '@mui/material';
import {
  CloudUpload as CloudUploadIcon,
  LinkedIn as LinkedInIcon,
} from '@mui/icons-material';
import axios from 'axios';

function a11yProps(index) {
  return {
    id: `search-tab-${index}`,
    'aria-controls': `search-tabpanel-${index}`,
  };
}

const SearchTabs = ({ selectedContacts, setSelectedContacts }) => {
  const [searches, setSearches] = useState([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [profiles, setProfiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [hubspotDialogOpen, setHubspotDialogOpen] = useState(false);
  const [hubspotResult, setHubspotResult] = useState(null);
  const [sendingToHubspot, setSendingToHubspot] = useState(false);
  const [selectAll, setSelectAll] = useState(false);

  useEffect(() => {
    fetchSearches();
  }, []);

  useEffect(() => {
    if (searches.length > 0) {
      setSelectedIndex(0);
      fetchProfiles(searches[0].id);
    }
  }, [searches]);

  const API_URL = process.env.REACT_APP_API_URL || 'http://143.244.155.153:5000';

  const fetchSearches = async () => {
    try {
      const response = await fetch(`${API_URL}/api/searches`);
      const data = await response.json();
      const sorted = data.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      setSearches(sorted);
    } catch (err) {
      setSearches([]);
    }
  };

  const fetchProfiles = async (searchId) => {
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/api/searches/${searchId}/profiles`);
      const data = await response.json();
      setProfiles(data);
    } catch (err) {
      setProfiles([]);
    }
    setLoading(false);
  };

  const handleTabChange = (event, newIndex) => {
    setSelectedIndex(newIndex);
    fetchProfiles(searches[newIndex].id);
  };

  const handleProfileSelect = (profile) => {
    setSelectedContacts(prev => {
      const isSelected = prev.some(c => c.id === profile.id);
      if (isSelected) {
        return prev.filter(c => c.id !== profile.id);
      } else {
        return [...prev, profile];
      }
    });
  };

  const handleSelectAll = () => {
    if (selectAll) {
      setSelectedContacts(prev => prev.filter(c => !profiles.some(p => p.id === c.id)));
      setSelectAll(false);
    } else {
      setSelectedContacts(prev => {
        const newOnes = profiles.filter(p => !prev.some(c => c.id === p.id));
        return [...prev, ...newOnes];
      });
      setSelectAll(true);
    }
  };

  const handleSendToHubspot = async () => {
    const selectedProfilesData = profiles.filter(p => selectedContacts.some(c => c.id === p.id));
    if (selectedProfilesData.length === 0) {
      return;
    }
    setSendingToHubspot(true);
    setHubspotDialogOpen(true);
    setHubspotResult(null);
    try {
      const response = await axios.post(`${API_URL}/api/send-to-hubspot`, {
        contacts: selectedProfilesData
      });
      setHubspotResult({
        success: true,
        message: `Se enviaron ${selectedProfilesData.length} contactos a HubSpot exitosamente.`
      });
    } catch (error) {
      setHubspotResult({
        success: false,
        message: error.response?.data?.error || 'Error al enviar a HubSpot'
      });
    } finally {
      setSendingToHubspot(false);
    }
  };

  const handleCloseHubspotDialog = () => {
    setHubspotDialogOpen(false);
    setHubspotResult(null);
  };

  const getStatusChip = (profile) => {
    if (profile.email || profile.mobileNumber) {
      return <Chip label="Contacto Encontrado" color="success" size="small" />;
    }
    if (profile.email_checked) {
      return <Chip label="Sin Contacto" color="error" size="small" />;
    }
    return <Chip label="Pendiente" color="warning" size="small" />;
  };

  return (
    <Box sx={{ width: '100%', mt: 4 }}>
      <Paper elevation={2} sx={{ mb: 2 }}>
        <Tabs
          value={selectedIndex}
          onChange={handleTabChange}
          variant="scrollable"
          scrollButtons="auto"
          aria-label="Búsquedas"
        >
          {searches.map((search, idx) => (
            <Tab
              key={search.id}
              label={search.name}
              {...a11yProps(idx)}
            />
          ))}
        </Tabs>
      </Paper>
      <Box>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
            <CircularProgress />
          </Box>
        ) : (
          <>
            {selectedContacts.length > 0 && (
              <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Typography variant="body2" color="text.secondary">
                  {selectedContacts.length} perfil(es) seleccionado(s)
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
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell padding="checkbox">
                      <Checkbox
                        checked={selectAll}
                        indeterminate={selectedContacts.length > 0 && selectedContacts.length < profiles.length}
                        onChange={handleSelectAll}
                      />
                    </TableCell>
                    <TableCell>Nombre</TableCell>
                    <TableCell>Título</TableCell>
                    <TableCell>Ubicación</TableCell>
                    <TableCell>Email</TableCell>
                    <TableCell>Teléfono</TableCell>
                    <TableCell>Estado</TableCell>
                    <TableCell>Acciones</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {profiles.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={8} align="center">
                        No hay resultados para esta búsqueda
                      </TableCell>
                    </TableRow>
                  ) : (
                    profiles.map((profile) => {
                      const isSelected = selectedContacts.some(c => c.id === profile.id);
                      return (
                        <TableRow key={profile.id}>
                          <TableCell padding="checkbox">
                            <Checkbox
                              checked={isSelected}
                              onChange={() => handleProfileSelect(profile)}
                            />
                          </TableCell>
                          <TableCell>
                            <Typography variant="subtitle2">{profile.fullName}</Typography>
                          </TableCell>
                          <TableCell>{profile.headline}</TableCell>
                          <TableCell>{profile.location}</TableCell>
                          <TableCell>
                            {profile.email && profile.email !== '' ? (
                              <Tooltip title={profile.email}>
                                <span>{profile.email}</span>
                              </Tooltip>
                            ) : 'No disponible'}
                          </TableCell>
                          <TableCell>
                            {profile.mobileNumber && profile.mobileNumber !== '' ? (
                              <Tooltip title={profile.mobileNumber}>
                                <span>{profile.mobileNumber}</span>
                              </Tooltip>
                            ) : 'No disponible'}
                          </TableCell>
                          <TableCell>{getStatusChip(profile)}</TableCell>
                          <TableCell>
                            {profile.profileUrl && (
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
                    })
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </>
        )}
      </Box>

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
    </Box>
  );
};

export default SearchTabs; 