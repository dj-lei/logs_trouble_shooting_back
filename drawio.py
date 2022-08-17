import uuid
import time
import configparser
from xml.etree import ElementTree
import xml.etree.cElementTree as ET
from algorithm import *
from utils import *

cf = configparser.ConfigParser()
cf.read('config/config.cfg')


##################################################
# Create data drive drawio xml
##################################################
class Drawio(object):
    def __init__(self, filter_class, filter_object, duplicate_object, highlight_keyword, story1_data, story2_data):
        self.filter_class = filter_class
        self.filter_object = filter_object
        self.duplicate_object = duplicate_object
        self.highlight_keyword = highlight_keyword
        self.story1_data = story1_data
        self.story2_data = story2_data

    def create_xml_header(self):
        mxfile = ET.Element("mxfile", host="Electron", modified=str(datetime.datetime.now()), agent="5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) draw.io/17.4.2 Chrome/100.0.4896.60 Electron/18.0.1 Safari/537.36", version="17.4.2", type="device")
        return mxfile

    def create_xml_diagram(self, mxfile, page_name):
        diagram_id = str(uuid.uuid4())
        diagram = ET.SubElement(mxfile, "diagram", name=page_name, id=diagram_id)
        mxGraphModel = ET.SubElement(diagram, "mxGraphModel", dx="1146", dy="597", grid="1", gridSize="10", guides="1", tooltips="1", connect="1", arrows="1", fold="1", page="1", pageScale="1", pageWidth="826", pageHeight="1169", background="none", math="0", shadow="0")
        root = ET.SubElement(mxGraphModel, "root")
        _ = ET.SubElement(root, "mxCell", id="0")
        _ = ET.SubElement(root, "mxCell", id="1", parent="0")
        return diagram_id, root

    def create_button(self, root, page_id, label,  x, y, width, height):
        UserObject = ET.SubElement(root, "UserObject", label=label, link="data:page/id," + page_id, id=str(uuid.uuid4()))
        mxCell = ET.SubElement(UserObject, "mxCell", style="whiteSpace=wrap;strokeWidth=2;fontSize=60;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": x, "y": y, "width": width, "height": height, "as": "geometry"})

    def create_objects_story_diff_header_graph(self, root, story_line_diff_id, length):
        # Background
        mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()), style="whiteSpace=wrap;strokeWidth=2;fillColor=#000000;strokeColor=#000000;shadow=0;", parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "0", "width": "4000", "height": str(length * int(cf['DRAWIO']['HEIGHT_DIFF']) * 2), "as": "geometry"})
        # Title text
        mxCell = ET.SubElement(root, "mxCell", value='<b style="font-size: 60px"> story1 VS story2 Structure Diff </b>', id=str(uuid.uuid4()), style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "-50", "as": "geometry"})
        # button story line
        self.create_button(root, story_line_diff_id, "story Line Diff", "0", "-200", "500", "80")

    def create_objects_elm_story_diff_graph(self, root, more_less_diff_id, i, sub_story_name, objects1_status, objects2_status):
        def create_diff_line(root, y, objects_status, name):
            flag = ''
            status = cf['DRAWIO']['COLOR_NORMAL']
            count = 0
            total_count = 0
            for status in objects_status:
                if flag == status:
                    count = count + 1
                else:
                    if flag != '':
                        mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()), style="whiteSpace=wrap;strokeWidth=0.5;fillColor=" + flag + ";strokeColor=" + flag + ";shadow=0;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
                        _ = ET.SubElement(mxCell, "mxGeometry", {"x": str(total_count), "y": y, "width": str(count), "height": "16" if flag == cf['DRAWIO']['COLOR_HIGHLIGHT'] else "12", "as": "geometry"})
                        total_count = total_count + count
                    count = 1
                    flag = status
            mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()),style="whiteSpace=wrap;strokeWidth=0.5;fillColor=" + status + ";strokeColor=" + status + ";shadow=0;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
            _ = ET.SubElement(mxCell, "mxGeometry", {"x": str(total_count), "y": y, "width": str(count), "height": "12", "as": "geometry"})
            text = ET.SubElement(root, "mxCell", value='<b>' + name + '</b>', id=str(uuid.uuid4()), style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
            _ = ET.SubElement(text, "mxGeometry", {"x": str(total_count + count + 5), "y": y, "width": "100", "height": "12", "as": "geometry"})

        UserObject = ET.SubElement(root, "UserObject", label='<b>' + sub_story_name + '</b>', link="data:page/id," + more_less_diff_id, id=str(uuid.uuid4()))
        mxCell = ET.SubElement(UserObject, "mxCell", style=cf['DRAWIO']['TEXT_STYLE'].replace('left', 'right') + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "-205", "y": str(i * int(cf['DRAWIO']['HEIGHT_DIFF']) * 2 + 5), "width": "200", "height": cf['DRAWIO']['HEIGHT'], "as": "geometry"})

        y1 = str(i * int(cf['DRAWIO']['HEIGHT_DIFF']) * 2)
        y2 = str(i * int(cf['DRAWIO']['HEIGHT_DIFF']) * 2 + 15)
        create_diff_line(root, y1, objects1_status, 'story1')
        create_diff_line(root, y2, objects2_status, 'story2')

    def create_story_line_diff_graph(self, root, home_id, story1_duration, story1_data, story2_data):
        def create_story_line(root, x, story_data, other_story_process):
            story = story_data
            story_line = []
            for i, process_name in enumerate(set(story.process.values)):
                process = story.loc[(story['process'] == process_name), :].reset_index(drop=True)
                process_start_time = process['timestamp'][0]
                process_end_time = process['timestamp'][process.shape[0] - 1]
                story_line.append([process_name, process_start_time, process_end_time])
            story_line = pd.DataFrame(story_line, columns=['process', 'process_start_time', 'process_end_time']).sort_values('process_start_time', ascending=True).reset_index(drop=True)

            start_time = min(story_line.process_start_time.values).split('.')[0]
            end_time = max(story_line.process_end_time.values).split('.')[0]
            duration = cal_time_difference(start_time, end_time)
            object_t = ET.SubElement(root, "object", label='TIMELINE', id=str(uuid.uuid4()))
            mxCell = ET.SubElement(object_t, "mxCell",style="whiteSpace=wrap;strokeWidth=2;fillColor=#000000;strokeColor=#000000;shadow=0;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
            _ = ET.SubElement(mxCell, "mxGeometry", {"x": str(x), "y": "0", "width": str(duration.seconds), "height": str((len(story_line)) * int(cf['DRAWIO']['HEIGHT_DIFF'])), "as": "geometry"})

            for i, (process, process_start_time, process_end_time) in enumerate(story_line[['process', 'process_start_time', 'process_end_time']].values):
                time_difference = cal_time_difference(process_start_time.split('.')[0], process_end_time.split('.')[0]).seconds
                distance_to_start = cal_time_difference(start_time, process_start_time.split('.')[0]).seconds
                y_axis = int(cf['DRAWIO']['HEIGHT_DIFF']) * i
                color = cf['DRAWIO']['COLOR_NORMAL_LINE'] if process in other_story_process else cf['DRAWIO']['COLOR_HIGHLIGHT']
                object_t = ET.SubElement(root, "object", label=process, ProcessStartTime=str(divmod(distance_to_start, 60)[0]) + ':' + str(divmod(distance_to_start, 60)[1]), Duration=str(divmod(time_difference, 60)[0]) + ':' + str(divmod(time_difference, 60)[1]), id=str(uuid.uuid4()))
                mxCell = ET.SubElement(object_t, "mxCell", style="whiteSpace=wrap;strokeWidth=2;fillColor=" + color + ";strokeColor=" + color + ";shadow=0;" + cf['DRAWIO']['COMMON_STYLE'].replace("editable=0;", ""), parent="1", vertex="1")
                _ = ET.SubElement(mxCell, "mxGeometry", {"x": str(x + distance_to_start), "y": str(y_axis), "width": str(time_difference if time_difference != 0 else 1), "height": cf['DRAWIO']['HEIGHT'], "as": "geometry"})

        # create home button
        self.create_button(root, home_id, "Home", "0", "-200", "200", "80")
        mxCell = ET.SubElement(root, "mxCell", value='<b style="font-size: 60px"> story Line Diff' + '</b>', id=str(uuid.uuid4()), style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "-50", "as": "geometry"})
        create_story_line(root, 0, story1_data, set(story2_data.process.values))
        create_story_line(root, story1_duration + 50, story2_data, set(story1_data.process.values))

    def create_objects_more_less_graph(self, root, home_id, value_diff_id, sub_story_name, object1, object2):
        # create home button
        self.create_button(root, home_id, "Home", "0", "-200", "200", "80")
        # create value diff button
        self.create_button(root, value_diff_id, "Key Value Diff", "220", "-200", "400", "80")
        # Title text
        mxCell = ET.SubElement(root, "mxCell", value='<b style="font-size: 60px">' + sub_story_name + ' More And Less Diff' + '</b>', id=str(uuid.uuid4()), style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "-50", "as": "geometry"})
        # Background
        mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()), style="whiteSpace=wrap;strokeWidth=2;fillColor=#000000;strokeColor=#000000;shadow=0;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "0", "width": "3000", "height": str(max([len(object1), len(object2)]) * float(cf['DRAWIO']['HEIGHT'])), "as": "geometry"})
        # Middle line
        mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()), value="", style="endArrow=none;html=1;rounded=0;fontColor=#00CC66;strokeWidth=5;" + cf['DRAWIO']['COMMON_STYLE'], edge="1", parent="1", vertex="1")
        mxGeometry = ET.SubElement(mxCell, "mxGeometry", {"width": "50", "height": "50", "relative": "1", "as": "geometry"})
        _ = ET.SubElement(mxGeometry, "mxPoint", {"x": "1100", "y": "0", "as": "sourcePoint"})
        _ = ET.SubElement(mxGeometry, "mxPoint", {"x": "1100", "y": str(max([len(object1), len(object2)]) * int(cf['DRAWIO']['HEIGHT_DIFF'])), "as": "targetPoint"})

        value1 = ''
        value2 = ''
        for i in range(0, max([len(object1), len(object2)])):
            if i < len(object1):
                value = '<font color= "' + object1['status'][i] + '"><b>' + object1['msg'][i] + '</b></font><br>'
            else:
                value = '<br>'
            value1 = value1 + value

            if i < len(object2):
                value = '<font color= "' + object2['status'][i] + '"><b>' + object2['msg'][i] + '</b></font><br>'
            else:
                value = '<br>'
            value2 = value2 + value
        mxCell1 = ET.SubElement(root, "mxCell", value=value1, id='object1_alone', style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell1, "mxGeometry", {"x": "0", "y": "0", "width": "800", "height": str(i * float(cf['DRAWIO']['HEIGHT'])), "as": "geometry"})
        mxCell2 = ET.SubElement(root, "mxCell", value=value2, id='object2_alone', style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell2, "mxGeometry", {"x": "1105", "y": "0", "width": "800", "height": str(i * float(cf['DRAWIO']['HEIGHT'])), "as": "geometry"})

    def create_objects_values_diff_graph(self, root, home_id, more_less_diff_id, sub_story_name, object1, object2):
        # create home button
        self.create_button(root, home_id, "Home", "0", "-200", "200", "80")
        # create more and less button
        self.create_button(root, more_less_diff_id, "More And Less Diff", "220", "-200", "600", "80")

        mxCell = ET.SubElement(root, "mxCell", value='<b style="font-size: 60px">' + sub_story_name + ' Value Diff' + '</b>', id=str(uuid.uuid4()), style=cf['DRAWIO']['TEXT_STYLE'] + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "-50", "as": "geometry"})

        mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()), style="whiteSpace=wrap;strokeWidth=2;fillColor=#000000;strokeColor=#000000;shadow=0;", parent="1", vertex="1")
        _ = ET.SubElement(mxCell, "mxGeometry", {"x": "0", "y": "0", "width": "3000", "height": str(len(object1) * float(cf['DRAWIO']['HEIGHT'])), "as": "geometry"})

        mxCell = ET.SubElement(root, "mxCell", id=str(uuid.uuid4()), value="", style="endArrow=none;html=1;rounded=0;fontColor=#00CC66;strokeWidth=5;" + cf['DRAWIO']['COMMON_STYLE'], edge="1", parent="1", vertex="1")
        mxGeometry = ET.SubElement(mxCell, "mxGeometry", {"width": "50", "height": "50", "relative": "1", "as": "geometry"})
        _ = ET.SubElement(mxGeometry, "mxPoint", {"x": "1100", "y": "0", "as": "sourcePoint"})
        _ = ET.SubElement(mxGeometry, "mxPoint", {"x": "1100", "y": str(max([len(object1), len(object2)]) * int(cf['DRAWIO']['HEIGHT_DIFF'])), "as": "targetPoint"})

        value1 = ''
        value2 = ''
        i = 0
        for i in range(0, len(object1)):
            if object1['msg'][i] != object2['msg'][i]:
                str1 = re.sub(" +", " ", object1['msg'][i]).split(' ')
                str2 = re.sub(" +", " ", object2['msg'][i]).split(' ')
                encoder1, encoder2 = onehot_encode_string(str1, str2)
                str_path, _ = cal_lcss_path_and_score(encoder1, encoder2)
                if len(str_path) != 0:
                    value_a = ' '.join(
                        [item if i in np.array(str_path)[:, 0] else cf['DRAWIO']['DIFF_KEYWORD'].replace('text', item) for i, item in
                         enumerate(str1)])
                    value_b = ' '.join(
                        [item if i in np.array(str_path)[:, 1] else cf['DRAWIO']['DIFF_KEYWORD'].replace('text', item) for i, item in
                         enumerate(str2)])
                else:
                    value_a = ' '.join([cf['DRAWIO']['DIFF_KEYWORD'].replace('text', item) for i, item in enumerate(str1)])
                    value_b = ' '.join([cf['DRAWIO']['DIFF_KEYWORD'].replace('text', item) for i, item in enumerate(str2)])
            else:
                value_a = object1['msg'][i]
                value_b = object2['msg'][i]
            value1 = value1 + '<b>' + value_a + '</b>' + '<br>'
            value2 = value2 + '<b>' + value_b + '</b>' + '<br>'

        mxCell1 = ET.SubElement(root, "mxCell", value=value1, id='object1_diff', style="text;html=1;align=left;verticalAlign=middle;resizable=0;points=[];autosize=1;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell1, "mxGeometry", {"x": "0", "y": "0", "width": "800", "height": str(i * float(cf['DRAWIO']['HEIGHT'])), "as": "geometry"})
        mxCell2 = ET.SubElement(root, "mxCell", value=value2, id='object2_diff', style="text;html=1;align=left;verticalAlign=middle;resizable=0;points=[];autosize=1;" + cf['DRAWIO']['COMMON_STYLE'], parent="1", vertex="1")
        _ = ET.SubElement(mxCell2, "mxGeometry", {"x": "1105", "y": "0", "width": "800", "height": str(i * float(cf['DRAWIO']['HEIGHT'])), "as": "geometry"})

    def create(self):
        # cal story duration
        story1_duration = cal_time_difference(min(self.story1_data.timestamp.values).split('.')[0], max(self.story1_data.timestamp.values).split('.')[0]).seconds

        # story2 unique sort, keep order
        sub_story_order = pd.unique(np.concatenate((self.story2_data.process.values, self.story1_data.process.values), axis=0))
        sub_story_order = np.delete(sub_story_order, np.where(sub_story_order == self.filter_object))

        # create story diff graph and create story line diff graph
        mxfile = self.create_xml_header()
        root_story_diff_id, root_story_diff = self.create_xml_diagram(mxfile, "story_Diff")
        root_story_line_diff_id, root_story_line_diff = self.create_xml_diagram(mxfile, "story_Line_Diff")
        self.create_objects_story_diff_header_graph(root_story_diff, root_story_line_diff_id, len(sub_story_order))
        self.create_story_line_diff_graph(root_story_line_diff, root_story_diff_id, story1_duration, self.story1_data, self.story2_data)

        time_start = time.time()
        for i, object_name in enumerate(sub_story_order):  # ['timeOutSrv'] sub_story_order[0:5]
            print(i, object_name)

            ##########################object1 init#############################
            object1 = self.story1_data.loc[(self.story1_data['process'] == object_name) & (~self.story1_data['fileAndLine'].isin(self.filter_class)), :].reset_index(drop=True)
            if len(object1) != 0:
                object1['is_filter'] = object1.apply(apply_filter_by_keywords, axis=1)
                object1 = object1.loc[(object1['is_filter'] == False), :].reset_index(drop=True)
            object1['status'] = cf['DRAWIO']['COLOR_NORMAL']
            if object_name in self.duplicate_object:
                object1 = object1.drop_duplicates(subset='msg').reset_index(drop=True)
            object1 = object1.reset_index().rename(columns={'index': 'original_index'})
            ###################################################################

            ##########################object2 init#############################
            object2 = self.story2_data.loc[(self.story2_data['process'] == object_name) & (~self.story2_data['fileAndLine'].isin(self.filter_class)), :].reset_index(drop=True)
            if len(object2) != 0:
                object2['is_filter'] = object2.apply(apply_filter_by_keywords, axis=1)
                object2 = object2.loc[(object2['is_filter'] == False), :].reset_index(drop=True)
            object2['status'] = cf['DRAWIO']['COLOR_NORMAL']
            if object_name in self.duplicate_object:
                object2 = object2.drop_duplicates(subset='msg').reset_index(drop=True)
            object2 = object2.reset_index().rename(columns={'index': 'original_index'})
            ##################################################################

            # lcss
            lcss_object1 = pd.DataFrame()
            lcss_object2 = pd.DataFrame()
            object1_data = object1.copy()
            object2_data = object2.copy()
            while True:
                if (len(object1_data) == 0) | (len(object2_data) == 0):
                    break
                encoder1, encoder2 = pretrained_model_encode_msg(object1_data, object2_data)
                path, score = cal_lcss_path_and_score(encoder1[1].cpu(), encoder2[1].cpu())
                if (len(path) == 0):
                    break

                lcss_object1 = lcss_object1.append(object1_data.loc[np.array(path)[:, 0], :]).reset_index(drop=True)
                object1_data = object1_data.drop(np.array(path)[:, 0], axis=0).reset_index(drop=True)
                lcss_object2 = lcss_object2.append(object2_data.loc[np.array(path)[:, 1], :]).reset_index(drop=True)
                object2_data = object2_data.drop(np.array(path)[:, 1], axis=0).reset_index(drop=True)

            object1.loc[object1_data.original_index.values, 'status'] = cf['DRAWIO']['COLOR_MORE_LESS']
            object2.loc[object2_data.original_index.values, 'status'] = cf['DRAWIO']['COLOR_MORE_LESS']
            object1['status'] = object1.apply(apply_keyword_highlight, args=(self.highlight_keyword, cf['DRAWIO']['COLOR_HIGHLIGHT'],), axis=1)
            object2['status'] = object2.apply(apply_keyword_highlight, args=(self.highlight_keyword, cf['DRAWIO']['COLOR_HIGHLIGHT'],), axis=1)

            # paint sub story more and less diff, value diff
            root_value_diff_id, root_value_diff = self.create_xml_diagram(mxfile, object_name + '_value')
            root_more_less_diff_id, root_more_less_diff = self.create_xml_diagram(mxfile, object_name + '_more_less')
            self.create_objects_values_diff_graph(root_value_diff, root_story_diff_id, root_more_less_diff_id, object_name, lcss_object1, lcss_object2)
            self.create_objects_more_less_graph(root_more_less_diff, root_story_diff_id, root_value_diff_id, object_name, object1, object2)

            # paint story diff graph
            self.create_objects_elm_story_diff_graph(root_story_diff, root_more_less_diff_id, i, object_name, object1.status.values, object2.status.values)

        time_end = time.time()
        print(time_end - time_start)
        tree = ET.ElementTree(mxfile)
        for diagram in tree.findall('diagram'):
            tmp = deflate_and_base64_encode(ElementTree.tostring(diagram.find('mxGraphModel'), method='xml'))
            diagram.remove(diagram.find('mxGraphModel'))
            diagram.text = tmp
        return tree


