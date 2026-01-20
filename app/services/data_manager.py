import json
import os
import logging

logger = logging.getLogger(__name__)

class DataManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
            cls._instance.bancos = {}
            cls._instance.meses = {}
            cls._instance._load_data()
        return cls._instance
    
    def _load_data(self):
        """Carrega os arquivos JSON da pasta app/data para a memória."""
        try:
            base_path = "app/data"

            bancos_path = f"{base_path}/bancos.json"
            if os.path.exists(bancos_path):
                with open(bancos_path, "r", encoding="utf-8") as f:
                    self.bancos = json.load(f)
            else:
                logger.warning(f"⚠️ [DataManager] Arquivo {bancos_path} não encontrado.")
            
            meses_path = f"{base_path}/meses.json"
            if os.path.exists(meses_path):
                with open(meses_path, "r", encoding="utf-8") as f:
                    self.meses = json.load(f)
            else:
                logger.warning(f"⚠️ [DataManager] Arquivo {meses_path} não encontrado.")

            logger.info(f"✅ [DataManager] Dados carregados: {len(self.bancos)} bancos, {len(self.meses)} meses.")
        
        except json.JSONDecodeError as e:
            logger.error(f"❌ [DataManager] Erro de sintaxe no JSON de bancos: {e}")
        except Exception as e:
            logger.error(f"❌ [DataManager] Erro inesperado ao carregar dados: {e}")
    
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
