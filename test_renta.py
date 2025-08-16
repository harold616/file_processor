# test_renta.py
from main import process_client_renta_data

def test_quick():
    csv_content = """C1;202504;1000;215940;76363085;4;1003;P;01;E;P;R;9034463;3
C1;202504;500;215940;76363085;4;1003;P;01;E;P;R;9034463;3
C1;202503;800;215940;76363085;4;1003;P;01;E;P;R;9034463;3"""
    
    result = process_client_renta_data(csv_content)
    print(result)
    
    # Debería mostrar: Cliente 9034463, periodo 202504, renta 1500

if __name__ == "__main__":
    test_quick()