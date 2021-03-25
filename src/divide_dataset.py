import os
import time

#@TODO: debug to check that packages from requirements.txt are installed
#import yaml
#import htmllistparse

import supervisely_lib as sly

#task_id = os.environ["TASK_ID"] # ----?
TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])
DATASET_ID = os.environ.get('modal.state.slyDatasetId', None)
COUNT_DATASETS = int(os.environ['modal.state.countDatasets'])
RESULT_PROJECT_NAME = os.environ["modal.state.projectName"]

my_app = sly.AppService()


@my_app.callback("divide_dataset")
@sly.timeit
def divide_dataset(api: sly.Api, task_id, context, state, app_logger):
    # Open exist src project
    src_project = api.project.get_info_by_id(PROJECT_ID)

    if src_project is None:
        print("Workspace {!r} not found".format(src_project.name))
    else:
        print(src_project)
    print()
  
    # Create result project
    if src_project.name == RESULT_PROJECT_NAME:
        print("You can't create {!r} project, because it already exist!".format(src_project.name))
    res_project = api.project.create(WORKSPACE_ID, RESULT_PROJECT_NAME)

    # Some information about count of images in src project and parts of dataset in result project
    count_images_in_scr_project = api.project.get_images_count(PROJECT_ID)  
    
    res_count_dataset = COUNT_DATASETS
    if COUNT_DATASETS > count_images_in_scr_project:
        res_count_dataset = count_images_in_scr_project
        
    count_images_in_dataset = count_images_in_scr_project // res_count_dataset
        
    # Add meta from src project to result project
    src_meta_json = api.project.get_meta(PROJECT_ID)
    src_meta = sly.ProjectMeta.from_json(src_meta_json)
    res_meta = src_meta
    api.project.update_meta(res_project.id, res_meta.to_json())

    # Get information from src dataset
    src_dataset_info = api.dataset.get_info_by_id(DATASET_ID)
    img_infos_all = api.image.get_list(DATASET_ID)

    # Create datasets and upload them by information fro src dataset
    temp_count_images = 0
    for index_dataset in range(res_count_dataset):
        if index_dataset == res_count_dataset - 1:  # case with remainder of division
            count_images_in_dataset += count_images_in_scr_project % res_count_dataset
        # Create dataset in result project
        dst_dataset = api.dataset.create(res_project.id, src_dataset_info.name + '_' + str(index_dataset))

        for img_infos in sly.batched(img_infos_all[temp_count_images:(temp_count_images + count_images_in_dataset)]):
            img_names, img_ids, img_metas = zip(*((x.name, x.id, x.meta) for x in img_infos))
            ann_infos = api.annotation.download_batch(src_dataset_info.id, img_ids)
            anns = [sly.Annotation.from_json(x.annotation, src_meta) for x in ann_infos]

            res_img_infos = api.image.upload_ids(dst_dataset.id, img_names, img_ids, metas=img_metas)
            res_img_ids = [x.id for x in res_img_infos]
            api.annotation.upload_anns(res_img_ids, anns)

        temp_count_images += count_images_in_dataset
  
    my_app.stop()


def main():
    api = sly.Api.from_env()

    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "PROJECT_ID": PROJECT_ID,
        "DATASET_ID": DATASET_ID,
        "countDatasets": COUNT_DATASETS
    })
    #initial_events = [{"state": None, "context": None, "command": "divide_dataset"}]
    # Run application service
    my_app.run(initial_events=[{"command": "divide_dataset"}])

if __name__ == "__main__":
    sly.main_wrapper("main", main)
