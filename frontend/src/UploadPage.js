import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Select, Button, Upload, message, Table } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import MainPage from './MainPage';

const { Option } = Select;

const UploadPage = () => {
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [availableTables, setAvailableTables] = useState([]);
  const [reportData, setReportData] = useState([]);
  const [reportColumns, setReportColumns] = useState([]);

  useEffect(() => {
    fetchTableOptions();
  }, []);

  const fetchTableOptions = async () => {
    try {
      const response = await axios.get('/etl/available-tables/');
      setAvailableTables(response.data.names);
    } catch (error) {
      console.error('Erro ao buscar as tabelas disponíveis:', error);
    }
  };

  const handleTableChange = (value) => {
    setSelectedTable(value);
    // Fazer requisição para buscar as colunas da tabela selecionada
    // e definir as colunas do relatório
    const mockReportColumns = [
      {
        title: 'key',
        dataIndex: 'key',
        key: 'key',
      },
      {
        title: 'table',
        dataIndex: 'table',
        key: 'table',
      },
      {
        title: 'Grupo',
        dataIndex: 'Grupo',
        key: 'Grupo',
      },
      {
        title: 'Linha',
        dataIndex: 'Linha',
        key: 'Linha',
      },
      {
        title: 'Fornecedor',
        dataIndex: 'Fornecedor',
        key: 'Fornecedor',
      },
      {
        title: 'Custo',
        dataIndex: 'Custo',
        key: 'Custo',
      },
    ];
    setReportColumns(mockReportColumns);
  };

  const handleFileChange = (info) => {
    if (info.file.status === 'done') {
      message.success(`${info.file.name} arquivo enviado com sucesso.`);
    } else if (info.file.status === 'error') {
      message.error(`${info.file.name} erro ao enviar o arquivo.`);
    }
    setSelectedFile(info.file.originFileObj);
  };

  const customRequest = ({ file, onSuccess, onError }) => {
    const formData = new FormData();
    formData.append('table_name', selectedTable);
    formData.append('file', file);

    axios
      .post('/etl/upload-arquivos/', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      })
      .then((response) => {
        if (response.data.success) {
          onSuccess(response.data, file);
          // Atualizar o relatório com os dados retornados da API
          setReportData(response.data.reportData); // Altere para o formato correto dos dados
        } else {
          onError(response.data.error);
        }
      })
      .catch((error) => {
        onError(error);
      });
  };

  return (
    <MainPage>
      <h1 style={{ color: '#fff' }}>Upload</h1>
      <div className="upload-form">
        <Select
          placeholder="Selecione uma tabela"
          value={selectedTable}
          onChange={handleTableChange}
          style={{ width: '100%', marginBottom: '10px', maxWidth: '200px', backgroundColor: '#333', color: '#fff' }}
        >
          {availableTables.map((table, index) => (
            <Option key={index} value={table}>
              {table}
            </Option>
          ))}
        </Select>
        <Upload
          customRequest={customRequest}
          onChange={handleFileChange}
          showUploadList={false}
          accept=".xlsx, .xls"
        >
          <Button icon={<UploadOutlined />} style={{ width: '100%', backgroundColor: '#333', color: '#fff' }}>
            Selecione o arquivo
          </Button>
        </Upload>
        <div style={{ color: '#fff', marginTop: '10px' }}>
          {selectedFile ? selectedFile.name : 'Nenhum arquivo escolhido'}
        </div>
        <div className="report" style={{ backgroundColor: '#000', marginTop: '20px', padding: '20px' }}>
          {/* Componente de relatório */}
          <Table dataSource={reportData} columns={reportColumns} theme="dark" />
        </div>
      </div>
    </MainPage>
  );
};

export default UploadPage;