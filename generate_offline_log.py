#coding=UTF-8

import random
import time


android_versions = {
	"Android 5.1",
	"Android 6",
	"Android 6.1",
	"Android 6.2",
	"Android x",
	"Android o",
	"Android 4.4",
	"Android 4.1",
}

phones = {
	"Meizu note 7",
	"Meizu 15",
	"Meizu 16s",
	"Meizu 16T",
	"Meizu 15 pro",
	"Meizu 16",
	"Meizu mx2",
	"Meizu mx3",
}

game_list = {
	13150:"王者荣耀",
	12110:"第二银河",
	131:"梦幻花园",
	2344:"闪烁之光",
	1144:"明日之后",
	1919:"猫和老鼠",
	1345:"崩坏3",
	104:"三国志",
	77:"部落冲突",
	114:"第五人格",
	3:"消零世界",
	5:"诛仙",
	10004:"奥拉星",
}

ip_slice = [132,156,124,19,29,167,143,187,30,40,55,66,72,87,98,100]

def sample_ip():
	slice = random.sample(ip_slice,4)
	return ".".join([str(item) for item in slice]) 



def randomtimes():
	a1=(2018,1,1,0,0,0,0,0,0)
	a2=(2018,12,31,23,59,59,0,0,0)
	start=time.mktime(a1)
	end=time.mktime(a2)
	t=random.randint(start,end)
	date_touple=time.localtime(t)
	date=time.strftime("%Y-%m-%d %H:%M:%S",date_touple)
	return date

def sample_av():
	return random.sample(android_versions,1)[0]

def sample_phone():
	if random.uniform(0,1) < 0.1:
		return "_"
	return random.sample(phones,1)[0]

def sample_gameId():
	return random.sample(list(game_list),1)[0]

def generate_log(count = 50000) :

	f = open("/Users/tianciyu/data/gc_offline.log","w+")
	while count >= 1:
		time_str = randomtimes()
		s_gameId = sample_gameId()
		s_gameName = game_list.get(s_gameId)
		query_log = "{ip}\t[{localtime}]\t({av},{phone})\t{gameName}\t{gameId}".format(ip=sample_ip(),localtime=time_str,av=sample_av(),phone=sample_phone(),gameName=s_gameName,gameId=s_gameId) 
		print(query_log)

		f.write(query_log + "\n")

		count = count - 1

if __name__ == '__main__':
	generate_log()















