from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat
from pathlib import Path
import pandas
import datetime
import time
from TheSetup import connect_me_with_the_setup
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper # https://github.com/SengerM/huge_dataframe
import datetime

def trigger_and_measure_dut_stuff(the_setup, name_to_access_to_the_setup:str, slots_numbers:list)->pandas.DataFrame:
	elapsed_seconds = 9999
	while elapsed_seconds > 5: # Because of multiple threads locking the different elements of the_setup, it can happen that this gets blocked for a long time. Thus, the measured data will no longer belong to a single point in time as we expect...:
		the_setup.wait_for_trigger(who=name_to_access_to_the_setup)
		trigger_time = time.time()
		stuff = []
		for slot_number in slots_numbers:
			measured_stuff = {
				'Bias voltage (V)': the_setup.measure_bias_voltage(slot_number),
				'Bias current (A)': the_setup.measure_bias_current(slot_number),
				'device_name': the_setup.get_name_of_device_in_slot_number(slot_number),
				'slot_number': slot_number,
				'When': datetime.datetime.now()
			}
			measured_stuff = pandas.DataFrame(measured_stuff, index=[0])
			stuff.append(measured_stuff)
		elapsed_seconds = trigger_time - time.time()
	return pandas.concat(stuff)

INDEX_COLUMNS = ['n_trigger','slot_number']

def acquire_test_beam_data(bureaucrat:RunBureaucrat, the_setup, name_to_access_to_the_setup:str, n_triggers:int, slots_numbers:list, silent:bool=True):
	with bureaucrat.handle_task('acquire_test_beam_data') as employee, \
		the_setup.hold_signal_acquisition(name_to_access_to_the_setup), \
		SQLiteDataFrameDumper(
			employee.path_to_directory_of_my_task/'waveforms.sqlite',
			dump_after_n_appends = 1111,
			dump_after_seconds = 11,
		) as waveforms_dumper, \
		SQLiteDataFrameDumper(
			employee.path_to_directory_of_my_task/'extra_stuff.sqlite',
			dump_after_n_appends = 1111,
			dump_after_seconds = 11,
		) as extra_stuff_dumper \
	:
		with open(employee.path_to_directory_of_my_task/'setup_description.txt', 'w') as ofile:
			print(the_setup.get_description(), file=ofile)
		n_trigger = -1
		while n_trigger < n_triggers:
			n_trigger += 1
			
			if not silent:
				print(f'Waiting for trigger in the oscilloscope...')
			measured_stuff = trigger_and_measure_dut_stuff(
				the_setup = the_setup,
				name_to_access_to_the_setup = name_to_access_to_the_setup,
				slots_numbers = slots_numbers,
			)
			measured_stuff['n_trigger'] = n_trigger
			extra_stuff_dumper.append(measured_stuff.set_index(INDEX_COLUMNS))
			
			if not silent:
				print(f'Acquiring n_trigger {n_trigger} out of {n_triggers}...')
			data = []
			for slot_number in slots_numbers:
				data = the_setup.get_waveform(the_setup.get_slots_configuration_df().loc[slot_number,'oscilloscope_channel_number'])
				for i in range(len(data)):
					if not silent:
						print(f'Processing n_trigger {n_trigger+i}/{n_triggers}, slot_number {slot_number}...')
					data[i]['n_trigger'] = n_trigger + i
					data[i]['slot_number'] = slot_number
					data[i] = pandas.DataFrame(data[i])
					waveforms_dumper.append(data[i].set_index(INDEX_COLUMNS))
			n_trigger = data[-1]['n_trigger'].max()

if __name__=='__main__':
	import os
	from configuration_files.current_run import Alberto
	from utils import create_a_timestamp
	
	SLOTS = [1,2,3,4]
	NAME_TO_ACCESS_TO_THE_SETUP = f'acquire test bean PID: {os.getpid()}'
	
	the_setup = connect_me_with_the_setup()
	
	with Alberto.handle_task('test_beam_data', drop_old_data=False) as employee:
		Mariano = employee.create_subrun(create_a_timestamp() + '_' + input('Measurement name? ').replace(' ','_'))
		acquire_test_beam_data(
			bureaucrat = Mariano,
			name_to_access_to_the_setup = NAME_TO_ACCESS_TO_THE_SETUP,
			n_triggers = 999,
			the_setup = the_setup,
			slots_numbers = SLOTS,
			silent = False,
		)

