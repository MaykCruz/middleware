import json
import os
import logging
import unicodedata

logger = logging.getLogger(__name__)

class DataManager:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.bancos = {}
        self.meses = {}
        self.estados_por_id_cidade = {}
        self.id_por_cidade_estado = {}
        self._load_data()
        self._initialized = True

    def _carregar_json(self, filepath: str, descricao: str):
        """Helper para carregar JSON de forma isolada e segura."""
        if not os.path.exists(filepath):
            logger.warning(f"⚠️ [DataManager] Arquivo {descricao} não encontrado em: {filepath}")
            return {}
        
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"❌ [DataManager] Erro de sintaxe (JSON inválido/vazio) em {descricao}: {e}")
            return {}
        except Exception as e:
            logger.error(f"❌ [DataManager] Erro genérico ao ler {descricao}: {e}")
            return {}

    def _load_data(self):
        """Carrega os arquivos JSON da pasta app/data."""
        base_path = "app/data"
        logger.info("🔄 [DataManager] Iniciando carregamento de dados...")

        # 1. Bancos
        self.bancos = self._carregar_json(f"{base_path}/bancos.json", "BANCOS")

        # 2. Meses
        self.meses = self._carregar_json(f"{base_path}/meses.json", "MESES")

        # 3. Cidades
        raw_cidades = self._carregar_json(f"{base_path}/cidades.json", "CIDADES")
        if raw_cidades:
            self._indexar_estados_cidades(raw_cidades)

        logger.info(f"✅ [DataManager] Carregamento finalizado. Bancos: {len(self.bancos)} | Meses: {len(self.meses)} | Cidades: {len(self.estados_por_id_cidade)}")
    
    def _normalizar_texto(self, texto: str) -> str:
        """Remove acentos, cedilhas e deixa tudo em maiúsculo."""
        if not texto:
            return ""
        texto_limpo = unicodedata.normalize('NFKD', str(texto)).encode('ASCII', 'ignore').decode('utf-8')
        return texto_limpo.upper().strip()
    
    def _indexar_estados_cidades(self, raw_data):
        """
        Lê o formato aninhado e cria um mapa ID -> UF.
        Entrada: { "cidade": { "197": {"nome": "X", "estado": "GO"}, ... } }
        Saída (estados_por_id_cidade): { "197": "GO", ... }
        """
        try:
            dados_cidades = raw_data.get("cidade", {})

            if not isinstance(dados_cidades, dict):
                logger.warning("⚠️ [DataManager] Formato de cidades.json inesperado.")
                return
            
            for cidade_id, info in dados_cidades.items():
                uf = str(info.get("estado", "")).upper().strip()
                nome_cidade = str(info.get("nome", ""))

                if uf:
                    self.estados_por_id_cidade[str(cidade_id)] = uf
                
                if nome_cidade and uf:
                    nome_norm = self._normalizar_texto(nome_cidade)
                    chave_busca = f"{nome_norm}|{uf}"
                    self.id_por_cidade_estado[chave_busca] = int(cidade_id)

        except Exception as e:
            logger.error(f"❌ [DataManager] Erro ao indexar cidades: {e}")
    
    def get_cidade_id(self, nome_cidade: str, uf: str) -> int:
        """Cruza o nome limpo e a UF para achar o ID da Facta."""
        if not nome_cidade or not uf:
            return None
        
        nome_norm = self._normalizar_texto(nome_cidade)
        uf_norm = str(uf).upper().strip()

        chave_busca = f"{nome_norm}|{uf_norm}"
        return self.id_por_cidade_estado.get(chave_busca)
    
    def get_uf_por_id(self, cidade_id: int) -> str:
        """
        Retorna a UF baseada no ID da cidade.
        Ex: Recebe 197 -> Retorna "GO"
        """
        if not cidade_id: return ""
        return self.estados_por_id_cidade.get(str(cidade_id), "")
    
    def get_nome_banco(self, codigo: str) -> str:
        """
        Busca o nome do banco pelo código.
        Ex: "001" -> "Banco do Brasil"
        """
        if not codigo:
            return ""
        
        code_str = str(codigo).strip().zfill(3)

        return self.bancos.get(code_str, f"Banco {code_str}")
    
    def get_nome_mes(self, mes: int) -> str:
        """
        Retorna o nome do mês por extenso.
        Ex: 1 -> 'janeiro', 12 -> 'dezembro'
        """
        key = str(mes)
        return self.meses.get(key, "")
