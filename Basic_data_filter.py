"""
    Module for basic stocks info filtering.
    Analyzed basic information based on current stock info retrieved from YF.

    Setting differrnt criteria
    one for dividend
    one for high growth

    # criteria type (required DictParser to parse the information)
    more: parameter > certain values
    less: parameter < certain values
    compare: parameter1 - parameter2 (>) certain value, where > or < depend on the compare type
    
    Required:
        Dict_create_fr_text

    Updates:
        Oct 05 2014: display filter criteria and qty after each filter.
                     Error handling when keys not present

    Learning:
        Dict comprehension
        http://stackoverflow.com/questions/1747817/python-create-a-dictionary-with-list-comprehension

        Adding a path to python env
        http://stackoverflow.com/questions/700375/how-to-add-a-python-import-path-using-a-pth-file
        
    TODO:
        note that the dict parser from site package
        Note some of the criteria are in str --> need to change to dec (need to convert)
        make it take a data file or a dataframe.
        iterate thro series of filter
        more defensive programming
        store each set of filtered data.
        Able to combine basic with advanced criteria
        settle on those percentage change...
        can take in dataframe or raw file.
        include Nan in filter --> fill the non zero value before --> some of the filter might not work correctly
        best to ignore htose that has no values

        develop subset criteria list

"""
import os, sys, re, time, datetime
import pandas
from DictParser.Dict_create_fr_text import DictParser

class InfoBasicFilter(object):
    """ Basic info filter by getting the curr stocks information for filtering.

    """
    def __init__(self, fname):
        """ Pass in the basic stock info for the analysis.

        """
        ## file path
        self.data_fname = fname
        self.criteria_type = '' # determine the type of criteria to use
        self.criteria_folder_path = r'C:\pythonuserfiles\yahoo_finance_data_extract\criteria'

        ## Load all the different criteria found in the self.criteria_folder_path.
        self.load_criteria_type_dict()

        ## output -- will be catered according to the input criteria type
        self.modified_raw_file_path = r'C:\data\stockpick'
        self.modified_fname = r''
        
        ## print options
        self.print_qty_left_aft_screen = 1 # if 1 will print the qty left after screening each fitler

        ## parameters -- create dataframe object
        self.data_df = pandas.read_csv(self.data_fname)

        ## options
        self.use_subset_criteria_list = 0 # if 1, will not run all the criteria found in path. Will get from self.sub_set_criteria_list
        self.subset_criteria_list = []

    def set_criteria_filepath(self, filepath):
        """ Set the criteria file path.
            Set to self.criteria_folder_path
            Args:
                filepath (str): path that store all the criteria.txt. 
        """
        self.criteria_folder_path =  filepath

    def load_criteria_type_dict(self):
        """ Create a dict of all the criteria type based on all the files in the folder.
            The dict key will be the file name.
            The folder path will based on the self.criteria_folder_path.
        """
        mypath  = self.criteria_folder_path
        self.criteria_type_path_dict = {os.path.splitext(f)[0]: os.path.join(mypath,f) for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath,f)) }

    def set_subset_criteria_list(self, subset_list):
        """ Set the subset_criteria_list.
            Use when the self.use_subset_criteria_list = 1.bit_length
            Args:
                subset_set (list): list of str containing the criteria name.
                
        """
        self.subset_criteria_list = subset_list

    def __print_criteria_info(self, criteria_type, *criteria_list):
        """ For printing the criteria information including the type of criteria parse, and the current criteria.
            For Debug and information.
            Args:
                criteria_type (str): type of critiera.
                criteria_list (additional args): separate list of critieral for printing.
        """
        print "Current Screen criteria: ", criteria_type, ' ', ','.join(criteria_list)

    def __print_modified_df_qty(self):
        """ Print the modified df qty.
            For Debug and information.
        """
        print "Modified_df qty: ", len(self.modified_df)

    def __print_snapshot_of_modified_df(self):
        """ Print a snapshot of some sample of the modified df.
        """
        print self.modified_df.head()

    def print_full_filter_on_criteria_type(self):
        """ Print all the filter present based on teh criteria type.
            Criteria type from the self.criteria_type.
        """

        print "List of filter for the criteria: ", self.criteria_type
        print '-'*40
        for n in ['greater', 'less','compare']:
            if not self.criteria_dict.has_key(n): continue #continue if criteria not found
            
            sub_criteria_dict = self.criteria_dict[n]
            if n == 'greater':
                for sub_n in sub_criteria_dict.keys():
                    print sub_n, ' > ', sub_criteria_dict[sub_n][0]

            if n == 'less':
                for sub_n in sub_criteria_dict.keys():
                    print sub_n, ' < ', sub_criteria_dict[sub_n][0]
                
            if n == 'compare':
                for sub_n in sub_criteria_dict.keys():
                    param_list =  sub_criteria_dict[sub_n]
                    if not len(param_list) == 4:
                        print 'Something wrong with compare criteria: ', param_list
                        continue
                    print param_list[0], param_list[2], param_list[1], param_list[3]
                    
        print
        
    def set_criteria_type(self, criteria_type):
        """ Set the criteria type. Criteria type must be one of the keys in the self.criteria_type_path_dict.
            Use to select the different criteria file.
            Args:
                criteria_type (str): criteria type
        """
        assert criteria_type in self.criteria_type_path_dict.keys()
        self.criteria_type =  criteria_type

    def print_all_availiable_criteria(self):
        """ Print all the criteria that is avaliable for filter.
            Take from the keys of the self.criteria_type_path_dictria

        """
        print self.criteria_type_path_dict.keys()

    def get_all_criteria_fr_file(self):
        """ Created in format of the dictparser.
            Dict parser will contain the greater, less than ,sorting dicts for easy filtering.
            Will parse according to the self.criteria_type

            Will also set the output file name
        """
        self.dictparser = DictParser(self.criteria_type_path_dict[self.criteria_type])
        self.criteria_dict = self.dictparser.dict_of_dict_obj
        self.modified_df = self.data_df
        ## fill the nan value
        self.modified_df.fillna(0, inplace =True)

        self.set_output_file()

    def set_output_file(self):
        """ Set the output file according to the critiera type chosen.

        """
        self.modified_fname = os.path.join(self.modified_raw_file_path, self.criteria_type + '_data.csv')
        
    def process_criteria(self):
        """ Process the different criteria generated.
            Present only have more and less

            TODO:
                split into smaller function.
                Take care of cases where the dict is not present.
        """
        greater_dict = dict()
        less_dict = dict()
        compare_dict = dict()
        print 'Processing each filter...'
        print '-'*40

        if self.criteria_dict.has_key('greater'): greater_dict =  self.criteria_dict['greater']
        if self.criteria_dict.has_key('less'): less_dict =  self.criteria_dict['less']
        if self.criteria_dict.has_key('compare'): compare_dict =  self.criteria_dict['compare']

        for n in greater_dict.keys():
            if not n in self.modified_df.columns: continue #continue if criteria not found
            self.modified_df = self.modified_df[self.modified_df[n] > float(greater_dict[n][0])]
            if self.print_qty_left_aft_screen:
                self.__print_criteria_info('Greater', n)
                self.__print_modified_df_qty()
                
        for n in less_dict.keys():
            if not n in self.modified_df.columns: continue #continue if criteria not found
            self.modified_df = self.modified_df[self.modified_df[n] < float(less_dict[n][0])]
            if self.print_qty_left_aft_screen:
                self.__print_criteria_info('Less',n)
                self.__print_modified_df_qty()

        for n in compare_dict.keys():
            first_item = compare_dict[n][0]
            sec_item = compare_dict[n][1]
            compare_type = compare_dict[n][2]
            compare_value = float(compare_dict[n][3])

            if not first_item in self.modified_df.columns: continue #continue if criteria not found
            if not sec_item in self.modified_df.columns: continue #continue if criteria not found

            if compare_type == 'greater':
                self.modified_df = self.modified_df[(self.modified_df[first_item] - self.modified_df[sec_item])> compare_value]
            elif compare_type == 'less':
                self.modified_df = self.modified_df[(self.modified_df[first_item] - self.modified_df[sec_item])< compare_value]

            if self.print_qty_left_aft_screen:
                self.__print_criteria_info('Compare',first_item, sec_item)
                self.__print_modified_df_qty()

        print 'END'
        print '\nSnapshot of final df ...'
        self.__print_snapshot_of_modified_df()

    def send_modified_to_file(self):
        """ Save the modified df to csv.

        """
        self.modified_df.to_csv(self.modified_fname, index = False)

    def loop_criteria(self):
        """ Loop all the criteria file (note have to reset the modified df)
            Also give some information on the header

        """
        for n in self.criteria_type_path_dict.keys():
            if self.use_subset_criteria_list:
                if not n in sub_set_criteria_list:
                    continue
            self.set_criteria_type(n)
            self.get_all_criteria_fr_file() #thsi is the problem
            self.print_full_filter_on_criteria_type()
            self.process_criteria()
            self.send_modified_to_file()

if __name__ == '__main__':
    #create a gui file
    choice =3

    print
    if choice ==1:
        print
        ss =  InfoBasicFilter(r'c:\data\full_aug30.csv')
        ss.set_criteria_type('potential')
        ss.get_all_criteria_fr_file()
        ss.process_criteria()
        ss.send_modified_to_file()

    if choice ==2:
        ss =  InfoBasicFilter(r'c:\data\full_oct02.csv')
        ss.set_criteria_type('potential')
        ss.get_all_criteria_fr_file()
        ss.print_full_filter_on_criteria_type()
        ss.process_criteria()
        #ss.modified_df.to_csv('c:\data\potential_low_PE.csv', index = False)
        #print ss.modified_df

    if choice ==3:
        ss =  InfoBasicFilter(r'C:\data\compile_stockdata\full_20150424.csv')
        ss.loop_criteria()

