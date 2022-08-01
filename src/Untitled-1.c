

status = H5Dwrite_async (dataset, H5T_NATIVE_INT, memspace, dataspace, data_transfer_propertylist, data,es_id);   

    
    async_dataset_write(async_instance_t *aid, H5VL_async_t *parent_obj, hid_t mem_type_id, hid_t mem_space_id,hid_t file_space_id, hid_t plist_id, 
    const void *buf, void **req)

        async_dataset_write_merge(aid,parent_obj,mem_type_id,mem_space_id,file_space_id,plist_id,buf);
                
                
                
                 DL_FOREACH(async_instance_g->qhead.queue,task_list_iter){
                    DL_FOREACH(task_list_iter->task_list, task_iter){
                        if(task_iter->func==async_dataset_write_fn){
                            //checking whether the iterator task is dataset write  operation
                            
                            if(task_iter->parent_obj == parent_obj){
                                //checking whether the iterator task is operating on the same dataset of the current task

                                iter_args = task_iter->args;
                                
                

                                fprintf(stderr,"%lld   %lld  %lld\n",task_iter->async_obj->under_object,iter_args->mem_space_id,iter_args->file_space_id);
                                fprintf(stderr,"For memory space:\n");
                                print_dataspace(iter_args->mem_space_id);
                                fprintf(stderr,"For file space:\n");
                                print_dataspace(iter_args->file_space_id);

                                check_contiguous(file_space_id,iter_args->file_space_id);

                                //1d,2d,3d

 1={0,5}
 2={5,3}
 3={8,2}
 4={10,10}    



 0  1  2  3  4  5
 6  7  8  9  10 11
 12 13 14 15 16 17
 18 19 20 21 22 23


 /* 
 0x0+2x2
 0x2+2x1
 0x3+2x2
 0x5+2x1
  */




