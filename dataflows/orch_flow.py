### Start
# Libraries
import datetime
import extract_CostOfLiving
import scrap_ListOfLatamCountries
import scrap_ListOfLanguages
import scrap_ListOfCurrencies
import load_gcs_to_bq_raw

# Terminal color messages
def prGreen(prt):
    """Print a green terminal message"""
    print(f"\033[92m{prt}\033[00m")
def prYellow(prt):
    """Print a yellow terminal message"""
    print(f"\033[93m{prt}\033[00m")
def prRed(prt):
    """Print a red terminal message"""
    print(f"\033[91m{prt}\033[00m")
def prCyan(prt):
    """Print a cyan terminal message"""
    print(f"\033[96m{prt}\033[00m")


### Tasks

### Flows
def orchestrator_main() -> None:
    """Orchestrator"""
    try:
        A = extract_CostOfLiving.main()
        prGreen(f'{datetime.datetime.now()} | OK | {A}')

        B = scrap_ListOfLatamCountries.main()
        prGreen(f'{datetime.datetime.now()} | OK | {B}')

        C = scrap_ListOfLanguages.main()
        prGreen(f'{datetime.datetime.now()} | OK | {C}')

        D = scrap_ListOfCurrencies.main()
        prGreen(f'{datetime.datetime.now()} | OK | {D}')

        X = load_gcs_to_bq_raw.main()
        prGreen(f'{datetime.datetime.now()} | OK | {X}')

    except Exception as e:
        raise Exception(f'{datetime.datetime.now()} | ERROR | Unexpected error: {e}')
    
    return

### Main
if __name__ == '__main__':
    orchestrator_main()

