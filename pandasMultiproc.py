import sys
import timeit as ti
import pandas as pd
import numpy as np
import getopt
from multiprocessing import Pool


def parallelize_df(data, func, num_cores):
    df_split = np.array_split(data, num_cores)
    pool = Pool(num_cores)
    data = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return data


def my_parallel_df(data: pd.DataFrame, func: callable, num_cores: int) -> pd.DataFrame:
    """function splits the DataFrame into smaller ones equaling the number of cores given and executes specified func
    on each split"""
    df_split = np.array_split(data, num_cores)
    with Pool(num_cores) as pool:
        # data = pd.concat(pool.map(func, df_split))
        output = pool.map(func, df_split)
        output = pd.concat(output)
        output.reset_index(inplace=True)
    return output


def make_it_upper(x: str) -> str:
    """
    convert the input string into uppercase
    :param x: string for conversion
    :return: converted string or x if it isn't string type
    """
    if type(x) == str:
        x = x.upper()
    return x


def sum_caps(word: str) -> int:
    """
    sum the number of capital letters in string
    :param word: input string
    :return: number of capital letters in word, if word isn't type string returns 0
    """
    return sum(1 for c in word if c.isupper()) if type(word) == str else 0


def count_occ_in_str(word: str, delim: str):
    """count words in a string split by delimiter"""
    return len(word.split(delim)) if type(word) == str else 0


def do_df_stuff(data: pd.DataFrame):
    """some sample operations on the DataFrame"""
    output_df = data.copy()
    output_df['Hobbyist'] = output_df['Hobbyist'].apply(make_it_upper)
    output_df['Gender'] = output_df['Gender'].apply(make_it_upper)
    output_df['InEuros'] = output_df['ConvertedComp'].apply(lambda x: x / 1.1574)
    output_df['EurosPerMo'] = output_df['InEuros'].apply(lambda x: x / 12)
    output_df['YearPlus13'] = output_df['EurosPerMo'] + output_df['InEuros']
    output_df['WorkWeekMin'] = output_df['WorkWeekHrs'].apply(lambda x: x * 60)

    output_df['MainBranchLen'] = output_df['MainBranch'].apply(lambda x: len(x) if type(x) == str else x)
    output_df['MainBranchCap'] = output_df['MainBranch'].apply(
        lambda x: sum(1 for c in x if c.isupper()) if type(x) == str else 0)
    output_df['MainBranchLow'] = output_df['MainBranch'].apply(
        lambda x: sum(1 for c in x if c.islower()) if type(x) == str else 0)
    output_df['MBCapPercent'] = output_df['MainBranchCap'] / output_df['MainBranchLen'] * 100
    output_df['MBWords'] = output_df['MainBranch'].apply(count_occ_in_str, args=' ')

    output_df['LangCount'] = output_df['LanguageWorkedWith'].apply(count_occ_in_str, args=';')
    output_df['NewContentCount'] = output_df['SONewContent'].apply(count_occ_in_str, args=';')
    output_df['SOVisitToWords'] = output_df['SOVisitTo'].apply(lambda x: x.split(';') if type(x) == str else [])
    output_df['SOVisitCount'] = output_df['SOVisitTo'].apply(count_occ_in_str, args=';')
    output_df['AgeInMonths'] = output_df['Age'].apply(lambda x: x * 12)
    output_df['MeanAge'] = output_df['Age'].mean()
    output_df['EurosPerMo'].median()
    output_df['Age'].value_counts()
    output_df.drop(['ConvertedComp', 'WorkWeekHrs', 'Age'], inplace=True, axis='columns')
    output_df['WorkWeekHrs'] = output_df['WorkWeekMin'].apply(lambda x: x / 60)
    output_df.drop('WorkWeekMin', inplace=True, axis='columns')

    countries = []
    """iterrows the slow way"""
    for i, r in output_df.iterrows():
        countries.append(r['Country'])

    """iterrows the fast way"""
    # dict_cpy = output_df.to_dict('records')
    # for r in dict_cpy:
    #     countries.append(r['Country'])

    output_df.dropna(inplace=True)
    output_df.reset_index(inplace=True)
    return output_df


def single_exec():
    """execute the func on dataframe in a single thread"""
    updated_df = do_df_stuff(df)
    if 'what_to_print' in globals():
        print(updated_df[what_to_print].tail())


def parallel_exec():
    """execute the func on dataframe in parallel"""
    cores_number = 4
    updated_df = my_parallel_df(df, do_df_stuff, cores_number)
    if 'what_to_print' in globals():
        print(updated_df[what_to_print].tail())


def main():
    """run the test function first as single thread and then as multi thread and measure the execution time"""
    print("Single core finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=single_exec, number=runs, globals=globals())))
    print("Multi core finished in {xtime} seconds.".format(
        xtime=ti.timeit(stmt=parallel_exec, number=runs, globals=globals())))


def usage():
    """
    prints the program usage
    """
    print('Pandas with multiprocessing demo program.\n'
          'Options:\n'
          '-h --help\tdisplay this help and exit the program\n'
          '-r --runs\tspecify the number of runs for each task\n'
          '-p --path\tspecify the path to the data file\n'
          'Usage:\n'
          'python pandasMultiproc.py <-h | -p path -r number>')


def parse_options() -> (int, str):
    """
    function for command line options parsing
    :return: pair runs, path
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], '-h -r: -p:', longopts=['help', 'runs=', 'path='])
    except getopt.GetoptError as ge:
        print(ge)
        usage()
        exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            exit(0)
        elif o in ('-r', '--runs'):
            try:
                runs = int(a)
            except ValueError as ve:
                print('Invalid number of runs: {}. Please give an integer.'.format(a))
                usage()
                exit(2)
        elif o in ('-p', '--path'):
            path = a
    try:
        path, runs
    except NameError as ne:
        print(ne)
        usage()
        exit(2)
    return runs, path


if __name__ == '__main__':
    runs, path = parse_options()
    df = pd.read_csv(path, index_col='Respondent')
    # control columns for print
    # what_to_print = ['MBCapPercent', 'MBWords', 'NewContentCount', 'LangCount', 'SOVisitCount']
    main()
