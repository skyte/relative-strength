import rs_data
import rs_ranking
import sys

def main():
   skipEnter = "false" if len(sys.argv) <= 1 else sys.argv[1]
   rs_data.main()
   rs_ranking.main(skipEnter=="true")

if __name__ == "__main__":
   main()