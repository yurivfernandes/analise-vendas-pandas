import React from 'react';
import { Layout, Menu } from 'antd';

const { Header, Content } = Layout;

const MainPage = ({ children }) => {
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header style={{ background: '#000', padding: '0', display: 'flex', justifyContent: 'flex-end' }}>
        <div className="menu-icon" style={{ fontSize: '24px', cursor: 'pointer', color: '#fff', paddingRight: '20px' }}>
          &#9776;
        </div>
      </Header>
      <Content style={{ backgroundColor: '#1a1a1a', padding: '20px', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
        <div className="content" style={{ backgroundColor: '#292929', borderRadius: '20px', width: '100%', maxWidth: '1200px', padding: '20px', boxShadow: '0px 0px 10px rgba(255, 255, 255, 0.1)' }}>
          {children}
        </div>
      </Content>
    </Layout>
  );
};

export default MainPage;