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
    // Simular a resposta da API com os dados do JSON manualmente
    const simulatedResponse = [
      {
        key: 0,
        'Tabela Alimentada': 'Produto',
        'N de Registros': '200',
        'Mensagem Adicional': 'Data início: 01-02-2022 | Data Fim: 01-02-2022',
        'Início da Carga': '01-02-2022',
        'Fim da carga': '01-02-2022',
        Status: 'Sucesso',
      },
      {
        key: 1,
        'Tabela Alimentada': 'Produto',
        'N de Registros': '200',
        'Mensagem Adicional': 'Data início: 01-02-2022 | Data Fim: 01-02-2022',
        'Início da Carga': '01-02-2022',
        'Fim da carga': '01-02-2022',
        Status: 'Sucesso',
      },
    ];
    setReportData(simulatedResponse);
    // Definir as colunas do relatório manualmente
    const simulatedColumns = [
      {
        title: 'key',
        dataIndex: 'key',
        key: 'key',
      },
      {
        title: 'Tabela Alimentada',
        dataIndex: 'Tabela Alimentada',
        key: 'Tabela Alimentada',
      },
      {
        title: 'N de Registros',
        dataIndex: 'N de Registros',
        key: 'N de Registros',
      },
      {
        title: 'Mensagem Adicional',
        dataIndex: 'Mensagem Adicional',
        key: 'Mensagem Adicional',
      },
      {
        title: 'Início da Carga',
        dataIndex: 'Início da Carga',
        key: 'Início da Carga',
      },
      {
        title: 'Fim da carga',
        dataIndex: 'Fim da carga',
        key: 'Fim da carga',
      },
      {
        title: 'Status',
        dataIndex: 'Status',
        key: 'Status',
      },
    ];
    setReportColumns(simulatedColumns);
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
    // setReportColumns([...]);
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
          setReportData(response.data.reportData);
          // Definir as colunas do relatório
          if (response.data.reportColumns) {
            setReportColumns(response.data.reportColumns);
          }
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
          <Table dataSource={reportData} columns={reportColumns} pagination={false} theme="dark" />
        </div>
      </div>
    </MainPage>
  );
};

export default UploadPage;
