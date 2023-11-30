import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import MainPage from './MainPage'; // Importe sua página principal
import UploadPage from './UploadPage'; // Importe sua página de upload

const AppRouter = () => {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<MainPage />} />
        <Route path="/upload" element={<UploadPage />} />
        {/* Defina mais rotas aqui, se necessário */}
      </Routes>
    </Router>
  );
};

export default AppRouter;