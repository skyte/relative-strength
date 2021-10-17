import rs_data
import rs_ranking
import sys

def main():
   skipEnter = None if len(sys.argv) <= 1 else sys.argv[1]
   forceTDA = None if len(sys.argv) <= 2 else sys.argv[2]
   api_key = None if len(sys.argv) <= 3 else sys.argv[3]
   if api_key:
      rs_data.main(forceTDA=="true", api_key)
   else:
      rs_data.main(forceTDA=="true")
   rs_ranking.main(skipEnter=="true")

if __name__ == "__main__":
   main()