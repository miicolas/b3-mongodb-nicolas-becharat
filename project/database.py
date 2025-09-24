from pymongo.mongo_client import MongoClient
from datetime import datetime
import uuid
import math


class Database:
    def __init__(self, connection_string, database_name):
        self.client = MongoClient(connection_string)
        self.db = self.client.get_database(database_name)

    def test_connection(self):
        try:
            return True
        except Exception:
            return False

    def _generate_pid(self):
        return str(uuid.uuid4())

    def _add_timestamps(self, item, created_by=None):
        now = datetime.utcnow()
        item['pid'] = self._generate_pid()
        item['created_at'] = now
        item['updated_at'] = now
        if created_by:
            item['created_by'] = created_by
        return item

    def create_item(self, table, item, created_by=None):
        collection = self.db.get_collection(table)
        item_with_meta = self._add_timestamps(item.copy(), created_by)
        result = collection.insert_one(item_with_meta)
        return item_with_meta['pid']

    def create_items(self, table, items, created_by=None):
        collection = self.db.get_collection(table)
        items_with_meta = []
        pids = []

        for item in items:
            item_with_meta = self._add_timestamps(item.copy(), created_by)
            items_with_meta.append(item_with_meta)
            pids.append(item_with_meta['pid'])

        collection.insert_many(items_with_meta)
        return pids

    def update_items_by_attr(self, table, attributes, items_data, updated_by=None):
        collection = self.db.get_collection(table)

        update_data = items_data.copy()
        update_data['updated_at'] = datetime.utcnow()
        if updated_by:
            update_data['updated_by'] = updated_by

        result = collection.update_many(attributes, {'$set': update_data})
        return result.modified_count
    
    def update_items_by_pids(self, table, pids, items_data, updated_by=None):
        collection = self.db.get_collection(table)

        update_data = items_data.copy()
        update_data['updated_at'] = datetime.utcnow()
        if updated_by:
            update_data['updated_by'] = updated_by

        result = collection.update_many({'pid': {'$in': pids}}, {'$set': update_data})
        return result.modified_count

    def update_item_by_attr(self, table, attributes, item_data, updated_by=None):
        collection = self.db.get_collection(table)

        update_data = item_data.copy()
        update_data['updated_at'] = datetime.utcnow()
        if updated_by:
            update_data['updated_by'] = updated_by

        result = collection.update_one(attributes, {'$set': update_data})
        return result.modified_count

    def update_item_by_pid(self, table, pid, item_data, updated_by=None):
        collection = self.db.get_collection(table)

        update_data = item_data.copy()
        update_data['updated_at'] = datetime.utcnow()
        if updated_by:
            update_data['updated_by'] = updated_by

        result = collection.update_one({'pid': pid}, {'$set': update_data})
        return result.modified_count

    def get_item_by_attr(self, table, attributes, fields=None, pipeline=None):
        collection = self.db.get_collection(table)

        agg_pipeline = []
        agg_pipeline.append({'$match': attributes})

        if pipeline:
            agg_pipeline.extend(pipeline)

        if fields:
            projection = {field: 1 for field in fields}
            agg_pipeline.append({'$project': projection})

        agg_pipeline.append({'$limit': 1})

        result = list(collection.aggregate(agg_pipeline))
        return result[0] if result else None

    def get_item_by_pid(self, table, pid, fields=None, pipeline=None):
        collection = self.db.get_collection(table)

        agg_pipeline = []
        agg_pipeline.append({'$match': {'pid': pid}})

        if pipeline:
            agg_pipeline.extend(pipeline)

        if fields:
            projection = {field: 1 for field in fields}
            agg_pipeline.append({'$project': projection})

        agg_pipeline.append({'$limit': 1})

        result = list(collection.aggregate(agg_pipeline))
        return result[0] if result else None

    def delete_items_by_attr(self, table, attributes):
        collection = self.db.get_collection(table)

        result = collection.delete_many(attributes)
        return result.deleted_count
    
    def delete_items_by_pids(self, table, pids):
        collection = self.db.get_collection(table)

        result = collection.delete_many({'pid': {'$in': pids}})
        return result.deleted_count
    
    def delete_item_by_attr(self, table, attributes):
        collection = self.db.get_collection(table)

        result = collection.delete_one(attributes)
        return result.deleted_count
    
    def delete_item_by_pid(self, table, pid):
        collection = self.db.get_collection(table)

        result = collection.delete_one({'pid': pid})
        return result.deleted_count
    
    def array_push_item_by_attr(self, table, attributes, array, new_item, updated_by=None):
        collection = self.db.get_collection(table)

        update_operation = {
            '$push': {array: new_item},
            '$set': {'updated_at': datetime.utcnow()}
        }
        if updated_by:
            update_operation['$set']['updated_by'] = updated_by

        result = collection.update_one(attributes, update_operation)
        return result.modified_count

    def array_push_item_by_pid(self, table, pid, array, new_item, updated_by=None):
        collection = self.db.get_collection(table)

        update_operation = {
            '$push': {array: new_item},
            '$set': {'updated_at': datetime.utcnow()}
        }
        if updated_by:
            update_operation['$set']['updated_by'] = updated_by

        result = collection.update_one({'pid': pid}, update_operation)
        return result.modified_count
    
    def array_pull_item_by_attr(self, table, attributes, array, item_attr, updated_by=None):
        collection = self.db.get_collection(table)

        update_operation = {
            '$pull': {array: item_attr},
            '$set': {'updated_at': datetime.utcnow()}
        }
        if updated_by:
            update_operation['$set']['updated_by'] = updated_by

        result = collection.update_one(attributes, update_operation)
        return result.modified_count
    
    def array_pull_item_by_pid(self, table, pid, array, item_attr, updated_by=None):
        collection = self.db.get_collection(table)

        update_operation = {
            '$pull': {array: item_attr},
            '$set': {'updated_at': datetime.utcnow()}
        }
        if updated_by:
            update_operation['$set']['updated_by'] = updated_by

        result = collection.update_one({'pid': pid}, update_operation)
        return result.modified_count
    
    def get_items(self, table, attributes, fields=None, sort=None, skip=0, limit=None, return_stats=False, pipeline=None):
        collection = self.db.get_collection(table)

        agg_pipeline = []
        agg_pipeline.append({'$match': attributes})

        if pipeline:
            agg_pipeline.extend(pipeline)

        if return_stats:
            count_pipeline = agg_pipeline.copy()
            count_pipeline.append({'$count': 'total'})
            count_result = list(collection.aggregate(count_pipeline))
            total_count = count_result[0]['total'] if count_result else 0

        if sort:
            agg_pipeline.append({'$sort': sort})

        if skip > 0:
            agg_pipeline.append({'$skip': skip})

        if limit:
            agg_pipeline.append({'$limit': limit})

        if fields:
            projection = {field: 1 for field in fields}
            agg_pipeline.append({'$project': projection})

        items = list(collection.aggregate(agg_pipeline))

        if return_stats:
            pages_count = math.ceil(total_count / limit) if limit else 1
            return {
                'items': items,
                'itemsCount': total_count,
                'pagesCount': pages_count,
                'firstIndexReturned': skip,
                'lastIndexReturned': min(skip + len(items), total_count)
            }
        return items

    def aggregate_stats(self, table, group_by, match_criteria=None):
        collection = self.db.get_collection(table)

        pipeline = []

        if match_criteria:
            pipeline.append({'$match': match_criteria})

        group_stage = {'_id': group_by, 'count': {'$sum': 1}}
        pipeline.append({'$group': group_stage})
        pipeline.append({'$sort': {'count': -1}})

        return list(collection.aggregate(pipeline))

    def aggregate_lookup(self, table, lookup_table, local_field, foreign_field, as_field):
        collection = self.db.get_collection(table)

        pipeline = [
            {
                '$lookup': {
                    'from': lookup_table,
                    'localField': local_field,
                    'foreignField': foreign_field,
                    'as': as_field
                }
            }
        ]

        return list(collection.aggregate(pipeline))

    def search_text(self, table, search_text, fields_to_search, limit=None):
        collection = self.db.get_collection(table)

        search_conditions = []
        for field in fields_to_search:
            search_conditions.append({field: {'$regex': search_text, '$options': 'i'}})

        query = {'$or': search_conditions}
        cursor = collection.find(query)

        if limit:
            cursor = cursor.limit(limit)

        return list(cursor)