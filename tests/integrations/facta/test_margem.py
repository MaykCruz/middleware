import logging
import math

# 1. Configurar o Python para exibir logs do nível DEBUG no console
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Test.FactaCLT")

class TesteCalculoCLT:
    def _definir_fator_margem(self, salario: float) -> float:
        """Cópia exata da sua regra de negócio."""
        if salario <= 5000.00:
            return 0.97
        elif salario <= 7350.00:
            return 0.90
        else:
            return 0.80

    def simular_margem(self, salario: float, margem: float):
        # Chama a função de fator
        fator_comprometimento = self._definir_fator_margem(salario)

        # Faz a conta e aplica o TRUNCAMENTO seguro (corta a cauda)
        parcela_maxima = round(margem * fator_comprometimento, 2)

        # O exato Logger que você tem no seu código
        logger.debug(f"💰 [CLT] Salário: {salario} | Fator: {fator_comprometimento} | Margem Líq: {margem} -> Comprometida: {parcela_maxima}")
        
        return parcela_maxima

if __name__ == "__main__":
    print("🚀 Iniciando testes de stress na Margem...\n")
    teste = TesteCalculoCLT()
    
    # CENÁRIO 1: O seu caso real (Salário mediano, Margem 95.14)
    # A conta bruta dá 85.626. Tem que aparecer 85.62 no log!
    print("--- CASO 1: Fator 90% ---")
    teste.simular_margem(salario=6000.00, margem=95.14)
    
    # CENÁRIO 2: Salário baixo (Fator 97%)
    # Margem 100.55 * 0.97 = 97.5335. Tem que aparecer 97.53
    print("\n--- CASO 2: Fator 97% ---")
    teste.simular_margem(salario=3500.00, margem=100.55)
    
    # CENÁRIO 3: Salário Alto (Fator 80%)
    # Margem 250.99 * 0.80 = 200.792. Tem que aparecer 200.79
    print("\n--- CASO 3: Fator 80% ---")
    teste.simular_margem(salario=8000.00, margem=250.99)
    
    print("\n✅ Teste concluído com sucesso!")