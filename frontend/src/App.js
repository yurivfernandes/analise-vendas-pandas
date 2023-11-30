import React, { useState } from 'react';
import axios from 'axios';
import './App.css'; // Importe o arquivo de estilos

function App() {
  const [loggedIn, setLoggedIn] = useState(false);

  // Aqui você pode adicionar a lógica de verificação de login, se necessário

  return (
    <div className="App">
      {loggedIn ? (
        <div className="main-container">
          <div className="top-menu">
            <div className="menu-icon">&#9776;</div>
          </div>
          {/* Aqui você pode adicionar o conteúdo da página */}
        </div>
      ) : (
        <div className="login-container">
          <h1>Login</h1>
          <input
            type="text"
            placeholder="Usuário"
          />
          <input
            type="password"
            placeholder="Senha"
          />
          <button onClick={() => setLoggedIn(true)}>Entrar</button>
        </div>
      )}
    </div>
  );
}

export default App;