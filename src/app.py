import json
import requests
from requests.auth import HTTPDigestAuth
from bs4 import BeautifulSoup
import boto3
import os
import re
import time
import random


def get_ancestor_page(ancestor_id: int) -> str:
    """
    Get the HTML individual page of a person, with her website id, from an HTTP request
    with digest authenfication.

    Parameters
    ----------
    ancestor_id : int
        The website id of the ancestor.

    Returns
    -------
    str
        The HTML page in str.
    """
    URL_base = "https://roglo.eu/roglo_f?lang=fr;i="
    try:
        request_answer = requests.get(
            URL_base + str(ancestor_id), auth=HTTPDigestAuth("X", "X")
        )
    except requests.RequestException as e:
        raise e
    return request_answer.text


def get_names(parsed_page: BeautifulSoup) -> tuple[str, str]:
    """
    Get the names (first and last) from an individual HTML page parsed
    with Beautiful Soup.
    We get the information from the HTML tree in a input tag.
    Ex :
    <input size="48" type="text"
        value="[[marguerite/de provence/0/Marguerite de Provence]]"/>

    Parameters
    ----------
    parsed_page : BeautifulSoup
        The HTML page parsed.

    Returns
    -------
    str
        The first name.
    str
        The last name.
    """
    first_input_tag = parsed_page.find_all("input")[0]
    tag_value = first_input_tag.get("value")
    tag_value_split = tag_value[2:].split("/")
    first_name = tag_value_split[0]
    last_name = tag_value_split[1]
    return first_name, last_name


def get_id(end_url: str) -> str:
    """
    Get the id in a specific str with a regex.

    Parameters
    ----------
    end_url : str
        The end of the url of a page.
        Ex : "roglo?lang=fr;i=1640524"

    Returns
    -------
    str
        The id.
    """
    return re.search("i=(\d+)", end_url).group(1)


def get_parent_id(parsed_page: BeautifulSoup) -> tuple[int, int]:
    """
    Get the parent ids from an individual HTML page parsed
    with Beautiful Soup.
    We get the information from the HTML tree near the first h3 tag.
    Ex : <h3 class="highlight">Parents</h3>

    Parameters
    ----------
    parsed_page : BeautifulSoup
        The HTML page parsed.

    Returns
    -------
    int
        The id of the father.
    int
        The id of the mother.
    """
    parents_list = list(parsed_page.find_all("h3")[0].next_sibling.next_sibling)
    father_query = parents_list[1].find("a").get("href")
    mother_query = parents_list[3].find("a").get("href")
    return int(get_id(father_query)), int(get_id(mother_query))


def test_parents_existence(parsed_page: BeautifulSoup) -> bool:
    """
    Test if a person has known parents from her HTML page parsed
    with Beautiful Soup.
    We get the information from the HTML tree in the first h3 tag.
    Ex : <h3 class="highlight">Parents</h3> -> True

    Parameters
    ----------
    parsed_page : BeautifulSoup
        The HTML page parsed.

    Returns
    -------
    bool
        True if exists parents, False else.
    """
    return "Parents" in parsed_page.find_all("h3")[0].text


def get_ancestor(ancestor_id: int) -> tuple[dict, bool]:
    """
    Get ancestor data from an id from roglo website,
    and check also if he has parents described.
    The data of the ancestor are saved in a dictionary.

    Parameters
    ----------
    ancestor_id : int
        The website id of the ancestor.

    Returns
    -------
    dict
        The data ancestor. The keys : id, html_source, first_name, last_name
        and eventually father_id, mother_id.

    bool
        True if the ancestor has parents described, False else.
    """
    ancestor_data = {}
    ancestor_data["id"] = ancestor_id
    ancestor_page = get_ancestor_page(ancestor_id)
    ancestor_data["html_source"] = ancestor_page
    parsed_page = BeautifulSoup(ancestor_page, "html.parser")
    ancestor_data["first_name"], ancestor_data["last_name"] = get_names(parsed_page)

    has_parents = test_parents_existence(parsed_page)
    if has_parents:
        father_id, mother_id = get_parent_id(parsed_page)
        ancestor_data["father_id"] = father_id
        ancestor_data["mother_id"] = mother_id
    return ancestor_data, has_parents


def test_ancestor_in_s3(
    s3: "boto3.client.S3", file_name: str, bucket_name: str
) -> bool:
    """
    Test if an ancestor represented by file_name exists already
    in the bucket bucket_name.
    The motivation is that an ancestor may appear several times in a family tree,
    leading to redundant processing.

    Parameters
    ----------
    s3 : boto3.client('s3')
        A low-level client S3 from boto3.

    Returns
    -------
    bool
        True if the ancestor has already been processed, False else.
    """
    ancestor_already_exists = True
    try:
        s3.head_object(
            Bucket=bucket_name,
            Key=file_name,
        )
    except:
        print("an except arrive")
        print("bucket_name:", bucket_name)
        print("file_name:", file_name)
        ancestor_already_exists = False
    return ancestor_already_exists


def save_to_s3(ancestor_data: dict, bucket_name: str) -> bool:
    """
    Save data of an ancestor to a bucket bucket_name in S3.
    A the condition, that he has not already been processed
    (see test_ancestor_in_s3 function).

    Parameters
    ----------
    ancestor_data : dict
        The data ancestor.

    bucket_name : str
        The name of the bucket where data ancestor are saved.

    Returns
    -------
    bool
        True if the ancestor has already been processed, False else.
    """
    first_name_formated = ancestor_data["first_name"].replace(" ", "-")
    last_name_formated = ancestor_data["last_name"].replace(" ", "-")
    file_name = (
        "_".join(
            [
                str(ancestor_data["level"]),
                str(ancestor_data["id"]),
                first_name_formated,
                last_name_formated,
            ]
        )
        + ".json"
    )
    s3 = boto3.client("s3")
    ancestor_already_exists = test_ancestor_in_s3(s3, file_name, bucket_name)
    if not ancestor_already_exists:
        json_str = json.dumps(ancestor_data)
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_str,
        )
    return ancestor_already_exists


def send_parentID_to_sqs(
    parent_id: int, level: int, maximum_level: int, queue_url: str
) -> None:
    """
    Send parent ids in a message to the main sqs queue,
    in order to processing them.

    Parameters
    ----------
    parent_id : int
        The id of a parent

    level : int
        The level in the recursivity tree (equivalent of depth).

    maximum_level: int
        The maximul level which controls the depth recursivity.

    queue_url : str
        The url of the processing sqs queue.
    """
    sqs = boto3.client("sqs")
    msg_body = {
        "ancestor_id": parent_id,
        "level": level,
        "maximum_level": maximum_level,
    }
    sqs.send_message(
        QueueUrl=queue_url,
        # DelaySeconds=10,
        MessageBody=(json.dumps(msg_body)),
    )


def lambda_handler(event, context):
    """
    Main function executed by AWS Lambda, which extracts a HTML page describing a person,
    from the roglo genealogy website. Then saves the page on S3.
    Finally, it adds potentially the person's parents to the processing queue.

    The variable maximum_level controls the ancestor generation where we stop,
    in other words it controls the depth recursivity.
    This limit can be specified in the event parameter, or by default it is 1,
    it means thus no recursivity.
    The maximum value possible is 16 with respect to the aws limit lambda recursion
    (see https://docs.aws.amazon.com/lambda/latest/dg/invocation-recursion.html).

    See https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html.

    Parameters
    ----------
    event : dict
        event is a JSON-formatted document that contains data
        for a Lambda function to process.

    context : object
        context object is passed to your function by Lambda at runtime.
        This object provides methods and properties that provide information
        about the invocation, function, and runtime environment.

    Returns
    -------
    dict
        The status code.
    """
    # (1) Message extraction
    msg_dic = json.loads(event["Records"][0]["body"])
    ancestor_id = msg_dic["ancestor_id"]
    print("ancestor_id :", ancestor_id)
    # maximum_level controls the recursivity, see the function description
    maximum_level = (
        msg_dic["maximum_level"] if "maximum_level" in list(msg_dic.keys()) else 1
    )
    if maximum_level > 16:
        return {"statusCode": 401}

    # (2) Extract data from the website
    ancestor_data, has_parents = get_ancestor(ancestor_id)
    ancestor_data["level"] = msg_dic["level"] + 1
    print(
        "first_name: ",
        ancestor_data["first_name"],
        "last_name: ",
        ancestor_data["last_name"],
        "has_parents: ",
        has_parents,
    )

    # (3) Save the data
    ancestor_already_exists = save_to_s3(ancestor_data, os.environ["bucketName"])
    print("ancestor_already_exists:", ancestor_already_exists)

    # (4) Recursivity stop conditions
    if (
        has_parents
        and not ancestor_already_exists
        and ancestor_data["level"] < maximum_level
    ):
        send_parentID_to_sqs(
            ancestor_data["father_id"],
            ancestor_data["level"],
            maximum_level,
            os.environ["mainQueueUrl"],
        )
        send_parentID_to_sqs(
            ancestor_data["mother_id"],
            ancestor_data["level"],
            maximum_level,
            os.environ["mainQueueUrl"],
        )

    # To avoid robo detection
    time.sleep(random.uniform(1, 2.5))

    return {"statusCode": 200}
